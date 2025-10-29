# ======================================
# Importaciones
# ======================================
import streamlit as st
import pandas as pd
from openpyxl import load_workbook, Workbook
from collections import defaultdict
from difflib import SequenceMatcher
from copy import deepcopy
import datetime
import io
import re
import time
from unidecode import unidecode
import numpy as np
from typing import List, Dict, Any
import gc # Importante para la gestión de memoria

# ### LIBRERÍAS PARA MODELOS LOCALES ###
# Se importarán dinámicamente para ahorrar memoria al inicio
# from transformers import AutoTokenizer, AutoModelForSequenceClassification
# from sentence_transformers import SentenceTransformer
# import torch
# from sklearn.metrics.pairwise import cosine_similarity

# ======================================
# Configuracion general
# ======================================
st.set_page_config(
    page_title="Análisis de Noticias UNAL",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ### CONFIGURACIÓN DE MODELOS ###
MODELO_SENTIMIENTO_LOCAL = "nlptown/bert-base-multilingual-uncased-sentiment"
MODELO_EMBEDDINGS_LOCAL = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"

# Marcas objetivo a analizar
TARGET_BRANDS = ["U. Nacional de Colombia", "Universidad Nacional de Colombia"]

# Parámetros
SIMILARITY_THRESHOLD_TITULOS = 0.90
SIMILARITY_THRESHOLD_RESUMENES = 0.85

# 30 TEMAS PREDEFINIDOS
TEMAS_PREDEFINIDOS = [
    "Elección y Gestión del Rector", "Decisiones del Consejo Superior Universitario", "Presupuesto y Financiación Universitaria", "Políticas y Reformas Administrativas", "Nombramientos y Cargos Directivos",
    "Proceso de Admisión y Aspirantes", "Desarrollo de Programas Académicos", "Investigaciones y Publicaciones Científicas", "Rankings y Acreditación Institucional", "Grados, Egresados y Ceremonias", "Colaboraciones y Convenios Académicos",
    "Protestas y Movilización Estudiantil", "Actividades y Grupos Estudiantiles", "Bienestar y Apoyo Estudiantil", "Asuntos de Representación Estudiantil", "Eventos Culturales y Deportivos",
    "Desarrollo de Infraestructura y Sedes", "Seguridad y Orden Público Campus", "Sostenibilidad y Medio Ambiente Campus", "Conectividad y Recursos Tecnológicos",
    "Aportes a Políticas Públicas", "Proyectos de Extensión y Comunidad", "Relación con el Gobierno Nacional", "Debates sobre Educación Superior", "Alianzas con Sector Privado",
    "Controversias y Denuncias Internas", "Reconocimientos y Premios Institucionales", "Egresados Destacados y Nombramientos", "Conflictos Laborales y Profesorado", "Relaciones con Egresados Alumni"
]
NUM_TEMAS = len(TEMAS_PREDEFINIDOS)

# ======================================
# Estilos CSS y Funciones de Utilidad (sin cambios significativos)
# ======================================
def load_custom_css():
    # ... (Tu CSS aquí)
    st.markdown("""<style>...</style>""", unsafe_allow_html=True)

def check_password() -> bool:
    # ... (Tu función de contraseña aquí)
    return True # Temporalmente para pruebas, reemplázalo con tu lógica

# (Aquí van el resto de tus funciones de utilidad: norm_key, extract_link, etc. No han cambiado)
def norm_key(text: Any) -> str:
    if text is None: return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

def extract_link(cell):
    if hasattr(cell, "hyperlink") and cell.hyperlink and cell.hyperlink.target:
        return {"value": "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        match = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if match: return {"value": "Link", "url": match.group(1)}
    return {"value": cell.value, "url": None}

def clean_title_for_output(title: Any) -> str:
    if not isinstance(title, str): return str(title if title is not None else "")
    return re.sub(r"\s*\|\s*[\w\s]+$", "", title).strip()

def corregir_texto(text: Any) -> Any:
    if not isinstance(text, str): return text if text is not None else ""
    text = re.sub(r'(<br\s*/?>|\[\.\.\.\])+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    match = re.search(r"[A-ZÁÉÍÓÚÑ]", text)
    if match: text = text[match.start():]
    if text and not text.endswith(('.', '...', '?', '!')): text = text + "..."
    return text

def normalizar_texto_para_comparacion(texto: Any) -> str:
    if not isinstance(texto, str): return ""
    texto = re.sub(r'[^\w\s]', '', unidecode(texto.lower()))
    return re.sub(r'\s+', ' ', texto).strip()

def calcular_similitud_textos(texto1: str, texto2: str) -> float:
    if not texto1 or not texto2: return 0.0
    return SequenceMatcher(None, texto1, texto2).ratio()

def normalizar_tipo_medio(tipo_raw: Any) -> str:
    if not isinstance(tipo_raw, str): return str(tipo_raw) if tipo_raw is not None else "Otro"
    t = unidecode(tipo_raw.strip().lower())
    mapping = {"fm": "Radio", "am": "Radio", "radio": "Radio", "aire": "Televisión", "cable": "Televisión", "tv": "Televisión", "television": "Televisión", "televisión": "Televisión", "senal abierta": "Televisión", "señal abierta": "Televisión", "diario": "Prensa", "prensa": "Prensa", "revista": "Revista", "revistas": "Revista", "online": "Internet", "internet": "Internet", "digital": "Internet", "web": "Internet"}
    return mapping.get(t, str(tipo_raw).strip().title() if str(tipo_raw).strip() else "Otro")

def format_tiempo(segundos: float) -> str:
    if segundos < 60: return f"{segundos:.1f} segundos"
    elif segundos < 3600: return f"{segundos / 60:.1f} minutos"
    else: return f"{segundos / 3600:.2f} horas"


# ======================================
# Lógica de Análisis Optimizada
# ======================================
# (Las funciones de agrupar, analizar_tono, asignar_tema, etc. no han cambiado)
def agrupa_noticias_similares_optimizado(noticias: List[Dict], key_map: Dict[str, str], status_update) -> Dict[int, List[int]]:
    status_update("Optimizando textos para comparación...")
    for i, noticia in enumerate(noticias):
        noticia['norm_titulo'] = normalizar_texto_para_comparacion(noticia.get(key_map.get('titulo'), ''))
        noticia['norm_resumen'] = normalizar_texto_para_comparacion(noticia.get(key_map.get('resumen'), ''))
        noticia['original_list_index'] = i

    status_update("Creando bloques de noticias para un análisis más rápido...")
    buckets = defaultdict(list)
    for i, noticia in enumerate(noticias):
        key = " ".join(noticia['norm_titulo'].split()[:5])
        if key: buckets[key].append(i)

    status_update(f"Comparando noticias dentro de {len(buckets)} bloques...")
    grupos = {}
    procesados = set()
    
    for i in range(len(noticias)):
        if i in procesados: continue
        grupo_actual = [i]; procesados.add(i)
        key_i = " ".join(noticias[i]['norm_titulo'].split()[:5])
        candidatos_indices = buckets.get(key_i, [])
        
        for j_cand in candidatos_indices:
            if j_cand <= i or j_cand in procesados: continue
            sim_titulo = calcular_similitud_textos(noticias[i]['norm_titulo'], noticias[j_cand]['norm_titulo'])
            if sim_titulo >= SIMILARITY_THRESHOLD_TITULOS:
                grupo_actual.append(j_cand); procesados.add(j_cand); continue
            sim_resumen = calcular_similitud_textos(noticias[i]['norm_resumen'], noticias[j_cand]['norm_resumen'])
            if sim_resumen >= SIMILARITY_THRESHOLD_RESUMENES:
                grupo_actual.append(j_cand); procesados.add(j_cand)
        grupos[i] = grupo_actual
    return grupos


def analizar_tono_batch(textos: List[str], tokenizer, model) -> List[str]:
    from transformers import torch
    if not textos: return []
    try:
        inputs = tokenizer(textos, return_tensors="pt", padding=True, truncation=True, max_length=512)
        with torch.no_grad(): logits = model(**inputs).logits
        scores = torch.argmax(logits, dim=-1).tolist()
        map_sentimiento = {0: "Negativo", 1: "Negativo", 2: "Neutro", 3: "Positivo", 4: "Positivo"}
        return [map_sentimiento.get(score, "Neutro") for score in scores]
    except Exception: return ["Neutro"] * len(textos)


def asignar_tema_batch(textos: List[str], modelo_emb, embeddings_temas) -> List[str]:
    from sklearn.metrics.pairwise import cosine_similarity
    import numpy as np
    if not textos: return []
    try:
        embeddings_noticias = modelo_emb.encode(textos, show_progress_bar=False, batch_size=32)
        similitudes = cosine_similarity(embeddings_noticias, embeddings_temas)
        indices = np.argmax(similitudes, axis=1)
        return [TEMAS_PREDEFINIDOS[idx] for idx in indices]
    except Exception: return ["Tema no asignado"] * len(textos)

# (Aquí van el resto de tus funciones de procesamiento: run_base_logic, etc. No han cambiado)
def run_base_logic(sheet, status_update):
    status_update("Leyendo encabezados del archivo...")
    headers = [c.value for c in sheet[1] if c.value]
    norm_keys = [norm_key(h) for h in headers]
    
    key_map = {nk: nk for nk in norm_keys}
    key_map.update({"titulo": norm_key("Titulo"), "resumen": norm_key("Resumen - Aclaracion"), "menciones": norm_key("Menciones - Empresa"), "medio": norm_key("Medio"), "tonoai": norm_key("Tono AI"), "justificaciontono": norm_key("Justificacion Tono"), "tema": norm_key("Tema"), "idnoticia": norm_key("ID Noticia"), "idduplicada": norm_key("ID duplicada"), "tipodemedio": norm_key("Tipo de Medio"), "hora": norm_key("Hora"), "link_nota": norm_key("Link Nota"), "link_streaming": norm_key("Link (Streaming - Imagen)"), "region": norm_key("Region")})
    
    status_update("Procesando filas de datos de forma segura...")
    rows = []
    for row_idx, row_cells in enumerate(sheet.iter_rows(min_row=2), start=2):
        if all(c.value is None for c in row_cells): continue
        
        row_data = dict(zip(norm_keys, row_cells))
        
        processed_row = {}
        for key, cell in row_data.items():
            if key in [key_map["link_nota"], key_map["link_streaming"]]:
                processed_row[key] = extract_link(cell)
            else:
                processed_row[key] = cell.value
        rows.append(processed_row)

    status_update(f"Se leyeron {len(rows)} filas. Dividiendo por menciones...")
    split_rows = []
    for base in rows:
        base[key_map["tipodemedio"]] = normalizar_tipo_medio(base.get(key_map["tipodemedio"]))
        menciones_str = str(base.get(key_map["menciones"], ""))
        menciones_list = [m.strip() for m in menciones_str.split(";") if m.strip()] or [menciones_str]
        for m in menciones_list:
            new = deepcopy(base); new[key_map["menciones"]] = m; split_rows.append(new)
    
    for idx, row in enumerate(split_rows): row.update({"original_index": idx, "is_duplicate": False})
    
    status_update("Detectando duplicados...")
    processed_rows = detectar_duplicados_avanzado(split_rows, key_map)
    for row in processed_rows:
        if row["is_duplicate"]: row.update({"tonoai": "Duplicada", "tema": "Duplicada", "justificaciontono": "Noticia duplicada."})
    return processed_rows, key_map

def detectar_duplicados_avanzado(rows: List[Dict], key_map: Dict[str, str]) -> List[Dict]:
    processed_rows = deepcopy(rows)
    seen_online_url, seen_broadcast = {}, {}
    for i, row in enumerate(processed_rows):
        if row.get("is_duplicate"): continue
        tipo_medio = normalizar_tipo_medio(str(row.get(key_map.get("tipodemedio"))))
        mencion_norm = norm_key(row.get(key_map.get("menciones")))
        medio_norm = norm_key(row.get(key_map.get("medio")))
        if tipo_medio == "Internet":
            url = (row.get(key_map.get("link_nota"), {}) or {}).get("url")
            if url and mencion_norm:
                key = (url, mencion_norm)
                if key in seen_online_url: row["is_duplicate"], row["idduplicada"] = True, processed_rows[seen_online_url[key]].get(key_map.get("idnoticia"), ""); continue
                else: seen_online_url[key] = i
        elif tipo_medio in ["Radio", "Televisión"]:
            hora = str(row.get(key_map.get("hora"), "")).strip()
            if mencion_norm and medio_norm and hora:
                key = (mencion_norm, medio_norm, hora)
                if key in seen_broadcast: row["is_duplicate"], row["idduplicada"] = True, processed_rows[seen_broadcast[key]].get(key_map.get("idnoticia"), ""); continue
                else: seen_broadcast[key] = i
    return processed_rows

def process_mappings_and_links(all_processed_rows, key_map, region_file, internet_file):
    df_region = pd.read_excel(region_file); region_map = {str(k).lower().strip(): v for k, v in pd.Series(df_region.iloc[:, 1].values, index=df_region.iloc[:, 0]).to_dict().items()}
    df_internet = pd.read_excel(internet_file); internet_map = {str(k).lower().strip(): v for k, v in pd.Series(df_internet.iloc[:, 1].values, index=df_internet.iloc[:, 0]).to_dict().items()}
    for row in all_processed_rows:
        original_medio_key = str(row.get(key_map.get("medio"), "")).lower().strip()
        row[key_map.get("region")] = region_map.get(original_medio_key, "N/A")
        if original_medio_key in internet_map: row[key_map.get("medio")], row[key_map.get("tipodemedio")] = internet_map[original_medio_key], "Internet"
    return all_processed_rows

def process_sov_mapping_final(all_rows: List[Dict], key_map: Dict[str, str], sov_file) -> List[Dict]:
    try:
        df_sov = pd.read_excel(sov_file)
        if df_sov.empty: return all_rows
        cols_by_norm = {norm_key(c): c for c in df_sov.columns}
        menc_col, name_col = cols_by_norm.get(norm_key("Menciones - Empresa")), cols_by_norm.get(norm_key("Nombre"))
        if not menc_col or not name_col: return all_rows
        sov_map = {str(r.get(menc_col, "")).strip().lower(): str(r.get(name_col)).strip() for _, r in df_sov.iterrows() if str(r.get(menc_col,"")).strip() and str(r.get(name_col,"")).strip()}
        if not sov_map: return all_rows
        for r in all_rows:
            mk = str(r.get(key_map.get("menciones"), "")).strip().lower()
            if mk in sov_map: r[key_map.get("menciones")] = sov_map[mk]
        return all_rows
    except Exception as e:
        st.warning(f"⚠️ No se pudo aplicar el mapeo SOV: {e}"); return all_rows

def generate_two_sheet_excel(all_processed_rows, key_map):
    out_wb = Workbook()
    sheet1 = out_wb.active; sheet1.title = "UNAL con IA"
    unal_rows = [row for row in all_processed_rows if row.get("__is_target_brand")]
    _append_rows_to_sheet(sheet1, unal_rows, key_map, include_ai_columns=True)
    sheet2 = out_wb.create_sheet("Todas las Marcas")
    _append_rows_to_sheet(sheet2, all_processed_rows, key_map, include_ai_columns=False)
    output = io.BytesIO(); out_wb.save(output)
    return output.getvalue()

def _append_rows_to_sheet(sheet, rows_data, key_map, include_ai_columns):
    base_order = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Seccion - Programa","Region","Titulo","Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Tier","Audiencia","Tono","Resumen - Aclaracion","Link Nota","Link (Streaming - Imagen)","Menciones - Empresa","ID duplicada"]
    ai_order = ["Tono AI", "Tema"]
    final_order = base_order[:16] + ai_order + base_order[16:] if include_ai_columns else base_order
    sheet.append(final_order)
    for row_data in rows_data:
        row_data[key_map.get("titulo")] = clean_title_for_output(row_data.get(key_map.get("titulo")))
        row_data[key_map.get("resumen")] = corregir_texto(row_data.get(key_map.get("resumen")))
        row_to_append, links_to_add = [], {}
        for col_idx, header in enumerate(final_order, 1):
            val = row_data.get(norm_key(header))
            cell_value = str(val) if val is not None else None
            if isinstance(val, dict) and "url" in val:
                cell_value, url = val.get("value", "Link"), val.get("url")
                if url: links_to_add[col_idx] = url
            row_to_append.append(cell_value)
        sheet.append(row_to_append)
        for col_idx, url in links_to_add.items():
            cell = sheet.cell(row=sheet.max_row, column=col_idx); cell.hyperlink = url; cell.style = "Hyperlink"


# ======================================
# Proceso Principal y UI (VERSIÓN FINAL)
# ======================================
def run_full_process(dossier_file, region_file, internet_file, sov_file):
    tiempo_inicio = time.time()
    
    with st.status("🚀 **Iniciando Análisis...**", expanded=True) as status:
        try:
            # --- PASO 1: Carga y Limpieza de Datos ---
            status.write("Paso 1/5: Preparando y limpiando datos...")
            all_processed_rows, key_map = run_base_logic(load_workbook(dossier_file, data_only=True).active, status.write)
            
            status.write("Aplicando mapeos de región e internet...")
            all_processed_rows = process_mappings_and_links(all_processed_rows, key_map, region_file, internet_file)
            
            for row in all_processed_rows:
                row["__is_target_brand"] = (row.get(key_map.get("menciones")) in TARGET_BRANDS)
            
            index_to_row_map = {row['original_index']: row for row in all_processed_rows}
            target_rows_all = [row for row in all_processed_rows if row.get("__is_target_brand") and not row.get("is_duplicate")]

            if not target_rows_all:
                st.warning("No se encontraron noticias únicas para las marcas objetivo."); status.update(label="⚠️ Sin noticias para procesar.", state="complete"); return
            
            # --- PASO 2: Agrupación ---
            status.update(label="Paso 2/5: Agrupando noticias similares...")
            grupos_similares = agrupa_noticias_similares_optimizado(target_rows_all, key_map, status.write)
            representantes_indices = list(grupos_similares.keys())
            noticias_representantes = [target_rows_all[i] for i in representantes_indices]
            status.write(f"Se identificaron {len(noticias_representantes)} grupos únicos de noticias.")
            
            textos_para_analisis = [f"{corregir_texto(rep.get(key_map.get('titulo'), ''))}. {corregir_texto(rep.get(key_map.get('resumen'), ''))}" for rep in noticias_representantes]

            # --- PASO 3: Análisis de Tono (con gestión de memoria) ---
            status.update(label="Paso 3/5: Analizando Tono con IA...")
            status.write("Cargando modelo de sentimiento...")
            from transformers import AutoTokenizer, AutoModelForSequenceClassification
            tokenizer_sent = AutoTokenizer.from_pretrained(MODELO_SENTIMIENTO_LOCAL)
            model_sent = AutoModelForSequenceClassification.from_pretrained(MODELO_SENTIMIENTO_LOCAL)
            
            status.write(f"Analizando tono para {len(textos_para_analisis)} grupos...")
            resultados_tono = analizar_tono_batch(textos_para_analisis, tokenizer_sent, model_sent)
            
            # Liberar memoria del modelo de sentimiento
            del tokenizer_sent, model_sent
            gc.collect()
            status.write("Memoria del modelo de sentimiento liberada.")

            # --- PASO 4: Análisis de Tema (con gestión de memoria) ---
            status.update(label="Paso 4/5: Asignando Temas con IA...")
            status.write("Cargando modelo de embeddings...")
            from sentence_transformers import SentenceTransformer
            modelo_emb = SentenceTransformer(MODELO_EMBEDDINGS_LOCAL)
            
            status.write("Generando embeddings para temas predefinidos...")
            embeddings_temas = modelo_emb.encode(TEMAS_PREDEFINIDOS, show_progress_bar=False)
            
            status.write(f"Asignando temas para {len(textos_para_analisis)} grupos...")
            resultados_tema = asignar_tema_batch(textos_para_analisis, modelo_emb, embeddings_temas)

            # Liberar memoria del modelo de embeddings
            del modelo_emb, embeddings_temas
            gc.collect()
            status.write("Memoria del modelo de embeddings liberada.")

            # --- PASO 5: Ensamblaje y Generación de Reporte ---
            status.update(label="Paso 5/5: Generando el informe final...")
            status.write("Asignando resultados a todos los miembros de cada grupo...")
            for i, idx_rep in enumerate(representantes_indices):
                tono, tema = resultados_tono[i], resultados_tema[i]
                for idx_miembro_lista in grupos_similares[idx_rep]:
                    original_global_index = target_rows_all[idx_miembro_lista]['original_index']
                    index_to_row_map[original_global_index][key_map.get("tonoai")] = tono
                    index_to_row_map[original_global_index][key_map.get("tema")] = tema

            final_processed_rows = list(index_to_row_map.values())
            final_processed_rows = process_sov_mapping_final(final_processed_rows, key_map, sov_file)
            
            st.session_state["output_data"] = generate_two_sheet_excel(final_processed_rows, key_map)
            st.session_state["output_filename"] = f"Informe_Analisis_UNAL_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            st.session_state["processing_complete"] = True
            st.session_state["tiempo_procesamiento"] = time.time() - tiempo_inicio
            
            status.update(label="✅ ¡Análisis Completado!", state="complete", expanded=False)

        except Exception as e:
            status.update(label=f"❌ Error Crítico: {e}", state="error", expanded=True)
            st.error(f"La aplicación ha encontrado un error. Por favor, revisa los archivos de entrada o contacta al soporte.")
            st.exception(e)
            st.session_state["processing_complete"] = False

# ======================================
# Interfaz de Usuario (main)
# ======================================
def main():
    load_custom_css()
    if not check_password(): return

    st.markdown('<div class="main-header">🎓 Sistema de Análisis de Noticias UNAL</div>', unsafe_allow_html=True)
    st.markdown(f"**Versión 12.0 (Modo Ligero)**: Esta herramienta utiliza modelos de IA locales para analizar Tono y clasificar noticias en **{NUM_TEMAS} categorías**.")
    
    with st.expander("📋 Ver los 30 temas predefinidos", expanded=False):
        # (Código para mostrar temas sin cambios)
        cols = st.columns(2)
        mitad = len(TEMAS_PREDEFINIDOS) // 2
        with cols[0]:
            for i, tema in enumerate(TEMAS_PREDEFINIDOS[:mitad], 1):
                st.markdown(f"**{i}.** {tema}")
        with cols[1]:
            for i, tema in enumerate(TEMAS_PREDEFINIDOS[mitad:], mitad + 1):
                st.markdown(f"**{i}.** {tema}")

    if not st.session_state.get("processing_complete", False):
        with st.form("input_form"):
            st.markdown("### 📂 Archivos de Entrada")
            col1, col2, col3, col4 = st.columns(4)
            dossier_file = col1.file_uploader("**1. Dossier Principal** (.xlsx)", type=["xlsx"])
            region_file = col2.file_uploader("**2. Mapeo de Región** (.xlsx)", type=["xlsx"])
            internet_file = col3.file_uploader("**3. Mapeo Internet** (.xlsx)", type=["xlsx"])
            sov_file = col4.file_uploader("**4. Mapeo SOV** (.xlsx)", type=["xlsx"])
            
            if st.form_submit_button("🚀 **INICIAR ANÁLISIS COMPLETO**", use_container_width=True, type="primary"):
                if not all([dossier_file, region_file, internet_file, sov_file]):
                    st.error("❌ Faltan archivos obligatorios.")
                else:
                    run_full_process(dossier_file, region_file, internet_file, sov_file)
                    st.rerun()
    else:
        st.success("## 🎉 Análisis Completado Exitosamente")
        
        if "tiempo_procesamiento" in st.session_state:
            tiempo_formateado = format_tiempo(st.session_state["tiempo_procesamiento"])
            st.markdown(f"""
            <div class="timer-box">
                <h2>⏱️ Tiempo Total de Procesamiento</h2>
                <p><strong>{tiempo_formateado}</strong></p>
            </div>
            """, unsafe_allow_html=True)
        
        st.download_button("📥 **DESCARGAR INFORME**", st.session_state.output_data, file_name=st.session_state.output_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")
        if st.button("🔄 **Realizar un Nuevo Análisis**", use_container_width=True):
            pwd = st.session_state.get("password_correct")
            st.session_state.clear()
            st.session_state.password_correct = pwd
            st.rerun()

    st.markdown("<hr><div style='text-align:center;color:#666;font-size:0.9rem;'><p>Sistema de Análisis de Noticias v12.0 (Modo Ligero) | Universidad Nacional de Colombia</p></div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()

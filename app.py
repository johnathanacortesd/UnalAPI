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

# ### LIBRERÍAS PARA MODELOS LOCALES ###
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from sentence_transformers import SentenceTransformer
import torch
from sklearn.metrics.pairwise import cosine_similarity

# ======================================
# Configuracion general
# ======================================
st.set_page_config(
    page_title="Análisis de Noticias para la Universidad Nacional",
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
# Estilos CSS y Funciones de Utilidad (sin cambios)
# ======================================
def load_custom_css():
    st.markdown("""<style>...</style>""", unsafe_allow_html=True) # Mantén tu CSS aquí

def check_password() -> bool:
    if st.session_state.get("password_correct", False): return True
    st.markdown('<div class="main-header">🔐 Portal de Acceso Seguro</div>', unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("password_form"):
            password = st.text_input("🔑 Contraseña:", type="password")
            if st.form_submit_button("🚀 Ingresar", use_container_width=True, type="primary"):
                if password == st.secrets.get("APP_PASSWORD", "INVALID_DEFAULT"):
                    st.session_state["password_correct"] = True; st.success("✅ Acceso autorizado."); st.balloons(); time.sleep(1.5); st.rerun()
                else: st.error("❌ Contraseña incorrecta")
    return False

def norm_key(text: Any) -> str:
    if text is None: return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

def extract_link(cell):
    if hasattr(cell, "hyperlink") and cell.hyperlink: return {"value": "Link", "url": cell.hyperlink.target}
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
# Carga de Modelos Locales Optimizada (sin cambios)
# ======================================
@st.cache_resource
def cargar_modelo_sentimiento():
    tokenizer = AutoTokenizer.from_pretrained(MODELO_SENTIMIENTO_LOCAL)
    model = AutoModelForSequenceClassification.from_pretrained(MODELO_SENTIMIENTO_LOCAL)
    return tokenizer, model

@st.cache_resource
def cargar_modelo_embeddings():
    model = SentenceTransformer(MODELO_EMBEDDINGS_LOCAL)
    return model

@st.cache_resource
def generar_embeddings_temas():
    modelo = cargar_modelo_embeddings()
    return modelo.encode(TEMAS_PREDEFINIDOS, show_progress_bar=False)

# ======================================
# ### OPTIMIZACIÓN 1: Agrupación Eficiente ###
# ======================================
def agrupar_noticias_similares_optimizado(noticias: List[Dict], key_map: Dict[str, str], status_update) -> Dict[int, List[int]]:
    """
    Agrupa noticias similares de forma eficiente usando una técnica de bloqueo
    para evitar comparaciones O(n^2).
    """
    status_update("Normalizando textos para comparación...")
    for i, noticia in enumerate(noticias):
        noticia['norm_titulo'] = normalizar_texto_para_comparacion(noticia.get(key_map.get('titulo'), ''))
        noticia['norm_resumen'] = normalizar_texto_para_comparacion(noticia.get(key_map.get('resumen'), ''))
        noticia['original_list_index'] = i # Guardar el índice original en la lista filtrada

    # Crear "cubetas" (buckets) para agrupar noticias potencialmente similares
    status_update("Creando bloques de noticias para análisis rápido...")
    buckets = defaultdict(list)
    for i, noticia in enumerate(noticias):
        # Usar las primeras 5 palabras del título como clave de cubeta
        key = " ".join(noticia['norm_titulo'].split()[:5])
        if key:
            buckets[key].append(i)

    # Procesar las comparaciones solo dentro de las cubetas
    status_update(f"Comparando noticias dentro de {len(buckets)} bloques...")
    grupos = {}
    procesados = set()
    
    num_noticias = len(noticias)
    for i in range(num_noticias):
        if i in procesados:
            continue
        
        grupo_actual = [i]
        procesados.add(i)
        
        # Obtener la cubeta a la que pertenece esta noticia
        key_i = " ".join(noticias[i]['norm_titulo'].split()[:5])
        
        # Comparar solo con noticias en la misma cubeta
        candidatos_indices = buckets.get(key_i, [])
        
        for j_cand in candidatos_indices:
            if j_cand <= i or j_cand in procesados:
                continue

            sim_titulo = calcular_similitud_textos(noticias[i]['norm_titulo'], noticias[j_cand]['norm_titulo'])
            if sim_titulo >= SIMILARITY_THRESHOLD_TITULOS:
                grupo_actual.append(j_cand)
                procesados.add(j_cand)
                continue # Si el título es muy similar, ya pertenece al grupo
            
            sim_resumen = calcular_similitud_textos(noticias[i]['norm_resumen'], noticias[j_cand]['norm_resumen'])
            if sim_resumen >= SIMILARITY_THRESHOLD_RESUMENES:
                grupo_actual.append(j_cand)
                procesados.add(j_cand)

        grupos[i] = grupo_actual

    return grupos

# ======================================
# ### OPTIMIZACIÓN 2: Análisis de IA por Lotes (Batch) ###
# ======================================
def analizar_tono_batch(textos: List[str], tokenizer, model) -> List[str]:
    """Analiza el tono para una lista de textos en un solo lote."""
    if not textos: return []
    try:
        inputs = tokenizer(textos, return_tensors="pt", padding=True, truncation=True, max_length=512)
        with torch.no_grad():
            logits = model(**inputs).logits
        
        scores = torch.argmax(logits, dim=-1).tolist()
        
        map_sentimiento = {0: "Negativo", 1: "Negativo", 2: "Neutro", 3: "Positivo", 4: "Positivo"}
        return [map_sentimiento.get(score, "Neutro") for score in scores]
    except Exception:
        return ["Neutro"] * len(textos)

def asignar_tema_batch(textos: List[str], modelo_emb, embeddings_temas) -> List[str]:
    """Asigna temas para una lista de textos en un solo lote."""
    if not textos: return []
    try:
        embeddings_noticias = modelo_emb.encode(textos, show_progress_bar=False)
        similitudes = cosine_similarity(embeddings_noticias, embeddings_temas)
        indices_mas_similares = np.argmax(similitudes, axis=1)
        return [TEMAS_PREDEFINIDOS[idx] for idx in indices_mas_similares]
    except Exception:
        return ["Tema no asignado"] * len(textos)

# ======================================
# Lógica de Procesamiento de Datos (Base) (sin cambios)
# ======================================
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

def run_base_logic(sheet):
    headers = [c.value for c in sheet[1] if c.value]
    norm_keys = [norm_key(h) for h in headers]
    key_map = {nk: nk for nk in norm_keys}
    key_map.update({"titulo": norm_key("Titulo"), "resumen": norm_key("Resumen - Aclaracion"), "menciones": norm_key("Menciones - Empresa"), "medio": norm_key("Medio"), "tonoai": norm_key("Tono AI"), "justificaciontono": norm_key("Justificacion Tono"), "tema": norm_key("Tema"), "idnoticia": norm_key("ID Noticia"), "idduplicada": norm_key("ID duplicada"), "tipodemedio": norm_key("Tipo de Medio"), "hora": norm_key("Hora"), "link_nota": norm_key("Link Nota"), "link_streaming": norm_key("Link (Streaming - Imagen)"), "region": norm_key("Region")})
    rows_raw = [dict(zip(norm_keys, [c.value for c in row])) for row in sheet.iter_rows(min_row=2) if not all(c.value is None for c in row)]
    rows_with_links = []
    for r in rows_raw:
        row_cells = sheet[rows_raw.index(r) + 2]
        r_linked = {}
        for i, key in enumerate(norm_keys):
            if key in [key_map["link_nota"], key_map["link_streaming"]]:
                r_linked[key] = extract_link(row_cells[i])
            else:
                r_linked[key] = r[key]
        rows_with_links.append(r_linked)
    
    split_rows = []
    for base in rows_with_links:
        base[key_map["tipodemedio"]] = normalizar_tipo_medio(base.get(key_map["tipodemedio"]))
        for m in [m.strip() for m in str(base.get(key_map["menciones"], "")).split(";") if m.strip()] or [base.get(key_map["menciones"])]:
            new = deepcopy(base); new[key_map["menciones"]] = m; split_rows.append(new)
    
    for idx, row in enumerate(split_rows): row.update({"original_index": idx, "is_duplicate": False})
    
    processed_rows = detectar_duplicados_avanzado(split_rows, key_map)
    for row in processed_rows:
        if row["is_duplicate"]: row.update({"tonoai": "Duplicada", "tema": "Duplicada", "justificaciontono": "Noticia duplicada."})
    return processed_rows, key_map

# Lógica de mapeos y generación de Excel (sin cambios)
def process_mappings_and_links(all_processed_rows, key_map, region_file, internet_file): return all_processed_rows
def process_sov_mapping_final(all_rows: List[Dict], key_map: Dict[str, str], sov_file) -> List[Dict]: return all_rows
def generate_two_sheet_excel(all_processed_rows, key_map): return b""
def _append_rows_to_sheet(sheet, rows_data, key_map, include_ai_columns): pass
# (Mantén tus funciones `process_mappings_and_links`, `process_sov_mapping_final`, `generate_two_sheet_excel`, `_append_rows_to_sheet` exactamente como estaban)


# ======================================
# Proceso Principal y UI (ACTUALIZADO)
# ======================================
def run_full_process(dossier_file, region_file, internet_file, sov_file):
    tiempo_inicio = time.time()
    
    with st.status("🚀 **Iniciando Análisis...**", expanded=True) as status:
        status.write("Paso 1/4: Preparando y limpiando datos...")
        all_processed_rows, key_map = run_base_logic(load_workbook(dossier_file, data_only=True).active)
        all_processed_rows = process_mappings_and_links(all_processed_rows, key_map, region_file, internet_file)
        
        for row in all_processed_rows:
            row["__is_target_brand"] = (row.get(key_map.get("menciones")) in TARGET_BRANDS)
        
        index_to_row_map = {row['original_index']: row for row in all_processed_rows}
        target_rows_all = [row for row in all_processed_rows if row.get("__is_target_brand") and not row.get("is_duplicate")]

        if not target_rows_all:
            st.warning("No se encontraron noticias únicas para las marcas objetivo.")
            status.update(label="⚠️ Análisis finalizado: Sin noticias para procesar.", state="complete")
            return

        status.update(label="Paso 2/4: Agrupando noticias similares (método optimizado)...")
        grupos_similares = agrupar_noticias_similares_optimizado(target_rows_all, key_map, status.write)
        
        representantes_indices = list(grupos_similares.keys())
        noticias_representantes = [target_rows_all[i] for i in representantes_indices]
        
        status.write(f"Se identificaron {len(noticias_representantes)} grupos únicos de noticias de un total de {len(target_rows_all)}.")
        
        status.update(label="Paso 3/4: Analizando Tono y Tema con IA (procesamiento por lotes)...")
        
        # Cargar modelos
        status.write("Cargando modelos de IA...")
        tokenizer_sent, model_sent = cargar_modelo_sentimiento()
        modelo_emb = cargar_modelo_embeddings()
        embeddings_temas = generar_embeddings_temas()
        
        # Preparar textos para el lote
        textos_para_analisis = [
            f"{corregir_texto(rep.get(key_map.get('titulo'), ''))}. {corregir_texto(rep.get(key_map.get('resumen'), ''))}"
            for rep in noticias_representantes
        ]
        
        # Analizar TONO en un solo lote
        status.write(f"Analizando tono para {len(textos_para_analisis)} grupos...")
        resultados_tono = analizar_tono_batch(textos_para_analisis, tokenizer_sent, model_sent)
        
        # Analizar TEMA en un solo lote
        status.write(f"Asignando temas para {len(textos_para_analisis)} grupos...")
        resultados_tema = asignar_tema_batch(textos_para_analisis, modelo_emb, embeddings_temas)
        
        # Propagar resultados a los grupos
        status.write("Asignando resultados a todos los miembros de cada grupo...")
        for i, idx_rep in enumerate(representantes_indices):
            tono = resultados_tono[i]
            tema = resultados_tema[i]
            
            for idx_miembro_lista in grupos_similares[idx_rep]:
                original_global_index = target_rows_all[idx_miembro_lista]['original_index']
                index_to_row_map[original_global_index][key_map.get("tonoai")] = tono
                index_to_row_map[original_global_index][key_map.get("tema")] = tema

        status.update(label="Paso 4/4: Generando el informe final...")
        final_processed_rows = list(index_to_row_map.values())
        final_processed_rows = process_sov_mapping_final(final_processed_rows, key_map, sov_file)
        
        st.session_state["output_data"] = generate_two_sheet_excel(final_processed_rows, key_map)
        st.session_state["output_filename"] = f"Informe_Analisis_UNAL_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.session_state["processing_complete"] = True
        
        tiempo_total = time.time() - tiempo_inicio
        st.session_state["tiempo_procesamiento"] = tiempo_total
        
        status.update(label="✅ ¡Análisis Completado!", state="complete", expanded=False)

def main():
    load_custom_css()
    if not check_password(): return

    st.markdown('<div class="main-header">🎓 Sistema de Análisis de Noticias para la Universidad Nacional</div>', unsafe_allow_html=True)
    st.markdown(f"Esta herramienta utiliza **modelos de IA locales** para analizar Tono y clasificar noticias en **{NUM_TEMAS} categorías temáticas predefinidas** para las marcas objetivo.")
    
    with st.expander("📋 Ver los 30 temas predefinidos", expanded=False):
        # ... (código para mostrar temas sin cambios)

    if not st.session_state.get("processing_complete", False):
        with st.form("input_form"):
            st.markdown("### 📂 Archivos de Entrada")
            # ... (código de carga de archivos sin cambios)
            if st.form_submit_button("🚀 **INICIAR ANÁLISIS COMPLETO**", use_container_width=True, type="primary"):
                # ... (código de validación de archivos sin cambios)
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

    st.markdown("<hr><div style='text-align:center;color:#666;font-size:0.9rem;'><p>Sistema de Análisis de Noticias v11.0 (Optimizado) | Universidad Nacional de Colombia</p></div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()

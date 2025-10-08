# ======================================
# Importaciones
# ======================================
import streamlit as st
import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, NamedStyle
from collections import defaultdict, Counter
from difflib import SequenceMatcher
from copy import deepcopy
import datetime
import io
import openai
import re
import time
from unidecode import unidecode
import numpy as np
from sklearn.cluster import AgglomerativeClustering
import json
import asyncio
import hashlib
from typing import List, Dict, Any, Optional
import gc

# ======================================
# Configuracion general
# ======================================
st.set_page_config(
    page_title="Análisis de Noticias para la Universidad Nacional",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Modelo de IA de alta precisión
OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14"

# Parámetros de rendimiento y similitud
CONCURRENT_REQUESTS = 20 # Reducido para priorizar la estabilidad con el nuevo enfoque
SIMILARITY_THRESHOLD_TITULOS = 0.95 
MAX_TOKENS_PROMPT_TXT = 6000
NUM_TEMAS_GENERALES = 30

# ======================================
# Estilos CSS
# ======================================
def load_custom_css():
    st.markdown(
        """
        <style>
        :root { --primary-color: #005A3A; --secondary-color: #B38612; --card-bg: #ffffff; --shadow-light: 0 2px 4px rgba(0,0,0,0.1); --border-radius: 12px; }
        .main-header { background: linear-gradient(135deg, var(--primary-color) 0%, var(--secondary-color) 100%); color: white; padding: 2rem; border-radius: var(--border-radius); text-align: center; font-size: 2.2rem; font-weight: 800; margin-bottom: 1.5rem; box-shadow: var(--shadow-light); }
        .stButton > button { border-radius: 8px; font-weight: 600; }
        </style>
        """,
        unsafe_allow_html=True,
    )

# ======================================
# Autenticación y Utilidades
# ======================================
def check_password() -> bool:
    if st.session_state.get("password_correct", False): return True
    st.markdown('<div class="main-header">🔐 Portal de Acceso Seguro</div>', unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("password_form"):
            password = st.text_input("🔑 Contraseña:", type="password")
            if st.form_submit_button("🚀 Ingresar", use_container_width=True, type="primary"):
                if password == st.secrets.get("APP_PASSWORD", "INVALID_DEFAULT"):
                    st.session_state["password_correct"] = True
                    st.success("✅ Acceso autorizado."); st.balloons(); time.sleep(1.5); st.rerun()
                else:
                    st.error("❌ Contraseña incorrecta")
    return False

async def acall_with_retries(api_func, *args, **kwargs):
    max_retries = 5; delay = 2
    for attempt in range(max_retries):
        try: return await api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                st.error(f"Error final en la API después de {max_retries} intentos: {e}")
                raise e
            st.warning(f"Intento {attempt + 1} fallido. Reintentando en {delay}s... Error: {e}")
            await asyncio.sleep(delay); delay *= 1.5

def call_with_retries(api_func, *args, **kwargs):
    max_retries = 3; delay = 1
    for attempt in range(max_retries):
        try: return api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            time.sleep(delay); delay *= 2

def norm_key(text: Any) -> str:
    if text is None: return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

def limpiar_tema(tema: str) -> str:
    if not tema: return "Sin tema"
    tema = tema.strip().strip('"').strip("'").strip(".").strip()
    if tema: tema = tema[0].upper() + tema[1:]
    return tema if tema else "Sin tema"

def extract_link(cell):
    if hasattr(cell, "hyperlink") and cell.hyperlink:
        return {"value": "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        match = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if match: return {"value": "Link", "url": match.group(1)}
    return {"value": cell.value, "url": None}

def normalize_text_for_comparison(text: Any) -> str:
    if not isinstance(text, str): return ""
    return re.sub(r"\W+", " ", unidecode(text).lower()).strip()

def clean_title_for_output(title: Any) -> str:
    if not isinstance(title, str): return str(title if title is not None else "")
    return re.sub(r"\s*\|\s*[\w\s]+$", "", title).strip()

def corregir_texto(text: Any) -> Any:
    if not isinstance(text, str): return text if text is not None else ""
    text = re.sub(r'(<br\s*/?>|\[\.\.\.\])+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    match = re.search(r"[\wÁÉÍÓÚÑ]", text) # Busca el primer caracter alfanumérico
    if match: text = text[match.start():]
    if text and not text.endswith(('.', '...', '?', '!')): text = text + "..."
    return text

def normalizar_tipo_medio(tipo_raw: str) -> str:
    if not isinstance(tipo_raw, str): return str(tipo_raw)
    t = unidecode(tipo_raw.strip().lower())
    mapping = {"fm": "Radio", "am": "Radio", "radio": "Radio", "aire": "Televisión", "cable": "Televisión", "tv": "Televisión", "television": "Televisión", "televisión": "Televisión", "senal abierta": "Televisión", "señal abierta": "Televisión", "diario": "Prensa", "prensa": "Prensa", "revista": "Revista", "revistas": "Revista", "online": "Internet", "internet": "Internet", "digital": "Internet", "web": "Internet"}
    default_value = str(tipo_raw).strip().title() if str(tipo_raw).strip() else "Otro"
    return mapping.get(t, default_value)

@st.cache_data(ttl=3600)
def get_embedding(texto: str) -> Optional[List[float]]:
    if not texto: return None
    try:
        resp = call_with_retries(openai.Embedding.create, input=[texto], model=OPENAI_MODEL_EMBEDDING)
        return resp["data"][0]["embedding"]
    except Exception: return None

# ======================================
# Análisis con IA (v9.0 - MÁXIMA PRECISIÓN)
# ======================================
class ClasificadorIA:
    def __init__(self, marca: str):
        self.marca = marca

    async def analizar_noticia_individual(self, texto_noticia: str, semaphore: asyncio.Semaphore) -> Dict[str, str]:
        async with semaphore:
            prompt = (
                f"Eres un editor de noticias senior para la '{self.marca}'. Tu trabajo es analizar la siguiente noticia y extraer información clave con máxima precisión. "
                "Responde OBLIGATORIAMENTE en formato JSON con dos claves:\n\n"
                "1. `tono`: Clasifica el sentimiento hacia la marca (Positivo, Negativo, Neutro).\n"
                "2. `tema_especifico`: Resume el evento principal en una frase corta y fáctica de 3 a 7 palabras. Este debe ser el núcleo de la noticia.\n\n"
                "**REGLAS CRÍTICAS:**\n"
                "- **PRECISIÓN FÁCTICA TOTAL:** Basa tu respuesta ESTRICTAMENTE en el texto proporcionado. NO INVENTES, NO GENERALICES, NO AÑADAS INFORMACIÓN EXTERNA.\n"
                "- **PRIORIDAD AL TÍTULO:** El Título es la fuente principal de verdad; el Resumen da contexto.\n"
                "- **NEUTRALIDAD:** No incluyas la marca, ciudades o gentilicios en el 'tema_especifico'.\n"
                "- **SI ES AMBIGUO:** Si el texto no permite determinar un evento claro, responde 'Análisis no concluyente' en 'tema_especifico'.\n\n"
                "--- NOTICIA A ANALIZAR ---\n"
                f"{texto_noticia[:MAX_TOKENS_PROMPT_TXT]}\n"
                "--- FIN DE LA NOTICIA ---\n\n"
                "JSON de respuesta:"
            )
            try:
                resp = await acall_with_retries(
                    openai.ChatCompletion.acreate, model=OPENAI_MODEL_CLASIFICACION,
                    messages=[{"role": "user", "content": prompt}], max_tokens=120, temperature=0.0,
                    response_format={"type": "json_object"}
                )
                data = json.loads(resp.choices[0].message.content.strip())
                tono = str(data.get("tono", "Neutro")).title()
                tema = limpiar_tema(data.get("tema_especifico", "Sin tema"))
                return {"tono": tono if tono in ["Positivo", "Negativo", "Neutro"] else "Neutro", "tema_especifico": tema}
            except Exception as e:
                return {"tono": "Error", "tema_especifico": f"Fallo en API"}

def consolidar_temas_generales(temas_especificos: List[str], p_bar) -> List[str]:
    p_bar.progress(0.85, text="📊 Sintetizando temas generales desde los específicos...")
    if not temas_especificos: return []
    
    valid_temas = [t for t in temas_especificos if t and t not in ["Sin tema", "Análisis no concluyente"]]
    if not valid_temas: return temas_especificos

    embeddings = {t: get_embedding(t) for t in set(valid_temas)}
    temas_con_emb = [t for t, emb in embeddings.items() if emb is not None]
    if len(temas_con_emb) < 2: return temas_especificos

    emb_matrix = np.array([embeddings[t] for t in temas_con_emb])
    n_clusters = min(NUM_TEMAS_GENERALES, len(temas_con_emb))
    clustering = AgglomerativeClustering(n_clusters=n_clusters, metric="cosine", linkage="average").fit(emb_matrix)
    
    mapa_especifico_a_general = {}
    mapa_cluster_a_temas = defaultdict(list)
    for i, label in enumerate(clustering.labels_):
        mapa_cluster_a_temas[label].append(temas_con_emb[i])
    
    for lista_temas in mapa_cluster_a_temas.values():
        tema_general = max(lista_temas, key=len) # El más descriptivo es el general
        for tema in lista_temas:
            mapa_especifico_a_general[tema] = tema_general
            
    p_bar.progress(1.0, "✅ Síntesis de temas completada.")
    return [mapa_especifico_a_general.get(t, t) for t in temas_especificos]

# ======================================
# Lógica de Duplicados, Mapeos y Excel
# ======================================
def detectar_duplicados_avanzado(rows: List[Dict], key_map: Dict[str, str]) -> List[Dict]:
    processed_rows = deepcopy(rows)
    seen_online_url, seen_broadcast, online_title_buckets = {}, {}, defaultdict(list)
    for i, row in enumerate(processed_rows):
        if row.get("is_duplicate"): continue
        tipo_medio = normalizar_tipo_medio(str(row.get(key_map.get("tipodemedio"))))
        mencion_norm = norm_key(row.get(key_map.get("menciones")))
        medio_norm = norm_key(row.get(key_map.get("medio")))
        if tipo_medio == "Internet":
            url = (row.get(key_map.get("link_nota"), {}) or {}).get("url")
            if url and mencion_norm:
                key = (url, mencion_norm)
                if key in seen_online_url:
                    row["is_duplicate"], row["idduplicada"] = True, processed_rows[seen_online_url[key]].get(key_map.get("idnoticia"), "")
                    continue
                else: seen_online_url[key] = i
            if medio_norm and mencion_norm: online_title_buckets[(medio_norm, mencion_norm)].append(i)
        elif tipo_medio in ["Radio", "Televisión"]:
            hora = str(row.get(key_map.get("hora"), "")).strip()
            if mencion_norm and medio_norm and hora:
                key = (mencion_norm, medio_norm, hora)
                if key in seen_broadcast:
                    row["is_duplicate"], row["idduplicada"] = True, processed_rows[seen_broadcast[key]].get(key_map.get("idnoticia"), "")
                else: seen_broadcast[key] = i
    for _, indices in online_title_buckets.items():
        if len(indices) < 2: continue
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                idx1, idx2 = indices[i], indices[j]
                if processed_rows[idx1].get("is_duplicate") or processed_rows[idx2].get("is_duplicate"): continue
                t1 = normalize_text_for_comparison(processed_rows[idx1].get(key_map.get("titulo")))
                t2 = normalize_text_for_comparison(processed_rows[idx2].get(key_map.get("titulo")))
                if t1 and t2 and SequenceMatcher(None, t1, t2).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                    winner, loser = (idx2, idx1) if len(t1) < len(t2) else (idx1, idx2)
                    processed_rows[loser]["is_duplicate"], processed_rows[loser]["idduplicada"] = True, processed_rows[winner].get(key_map.get("idnoticia"), "")
    return processed_rows

def run_base_logic(sheet):
    headers = [c.value for c in sheet[1] if c.value]
    norm_keys = [norm_key(h) for h in headers]
    key_map = {nk: nk for nk in norm_keys}
    key_map.update({"titulo": norm_key("Titulo"), "resumen": norm_key("Resumen - Aclaracion"), "menciones": norm_key("Menciones - Empresa"), "medio": norm_key("Medio"), "tonoai": norm_key("Tono AI"), "temageneral": norm_key("Tema General"), "temaespecifico": norm_key("Tema Específico"), "idnoticia": norm_key("ID Noticia"), "idduplicada": norm_key("ID duplicada"), "tipodemedio": norm_key("Tipo de Medio"), "hora": norm_key("Hora"), "link_nota": norm_key("Link Nota"), "link_streaming": norm_key("Link (Streaming - Imagen)"), "region": norm_key("Region")})
    rows = [{norm_keys[i]: c for i, c in enumerate(row) if i < len(norm_keys)} for row in sheet.iter_rows(min_row=2) if not all(c.value is None for c in row)]
    split_rows = []
    for r_cells in rows:
        base = {k: extract_link(v) if k in [key_map["link_nota"], key_map["link_streaming"]] else v.value for k, v in r_cells.items()}
        base[key_map["tipodemedio"]] = normalizar_tipo_medio(base.get(key_map["tipodemedio"]))
        m_list = [m.strip() for m in str(base.get(key_map["menciones"], "")).split(";") if m.strip()]
        for m in m_list or [base.get(key_map["menciones"])]:
            new = deepcopy(base); new[key_map["menciones"]] = m
            split_rows.append(new)
    for idx, row in enumerate(split_rows): row.update({"original_index": idx, "is_duplicate": False})
    processed_rows = detectar_duplicados_avanzado(split_rows, key_map)
    for row in processed_rows:
        if row["is_duplicate"]: row.update({key_map["tonoai"]: "Duplicada", key_map["temageneral"]: "Duplicada", key_map["temaespecifico"]: "Duplicada"})
    return processed_rows, key_map

def process_mappings_and_links(all_processed_rows, key_map, region_file, internet_file):
    df_region = pd.read_excel(region_file)
    region_map = {str(k).lower().strip(): v for k, v in pd.Series(df_region.iloc[:, 1].values, index=df_region.iloc[:, 0]).to_dict().items()}
    df_internet = pd.read_excel(internet_file)
    internet_map = {str(k).lower().strip(): v for k, v in pd.Series(df_internet.iloc[:, 1].values, index=df_internet.iloc[:, 0]).to_dict().items()}
    for row in all_processed_rows:
        original_medio_key = str(row.get(key_map.get("medio"), "")).lower().strip()
        row[key_map.get("region")] = region_map.get(original_medio_key, "N/A")
        if original_medio_key in internet_map:
            row[key_map.get("medio")] = internet_map[original_medio_key]
            row[key_map.get("tipodemedio")] = "Internet"
        tkey, ln_key, ls_key = key_map.get("tipodemedio"), key_map.get("link_nota"), key_map.get("link_streaming")
        if tkey and ln_key and ls_key:
            tipo, ln, ls = row.get(tkey, ""), row.get(ln_key) or {}, row.get(ls_key) or {}
            has_url = lambda x: isinstance(x, dict) and bool(x.get("url"))
            if tipo in ["Radio", "Televisión"]: row[ls_key] = {"value": "", "url": None}
            elif tipo == "Internet": row[ln_key], row[ls_key] = ls, ln
            elif tipo in ["Prensa", "Revista"]:
                if not has_url(ln) and has_url(ls): row[ln_key] = ls
                row[ls_key] = {"value": "", "url": None}
    return all_processed_rows

def _append_rows_to_sheet(sheet, rows_data, key_map, include_ai_columns):
    base_order = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Seccion - Programa","Region","Titulo","Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Tier","Audiencia","Tono","Resumen - Aclaracion","Link Nota","Link (Streaming - Imagen)","Menciones - Empresa","ID duplicada"]
    ai_order = ["Tono AI", "Tema Específico", "Tema General"]
    final_order = base_order[:16] + ai_order + base_order[16:] if include_ai_columns else base_order
    sheet.append(final_order)
    numeric_columns = {"ID Noticia", "Nro. Pagina", "Dimension", "Duracion - Nro. Caracteres", "CPE", "Tier", "Audiencia"}
    for row_data in rows_data:
        row_data[key_map.get("titulo")] = clean_title_for_output(row_data.get(key_map.get("titulo")))
        row_data[key_map.get("resumen")] = corregir_texto(row_data.get(key_map.get("resumen")))
        row_to_append, links_to_add = [], {}
        for col_idx, header in enumerate(final_order, 1):
            val = row_data.get(norm_key(header))
            cell_value = None
            if header in numeric_columns:
                try: cell_value = float(val) if val is not None and str(val).strip() != "" else None
                except (ValueError, TypeError): cell_value = str(val)
            elif isinstance(val, dict) and "url" in val:
                cell_value, url = val.get("value", "Link"), val.get("url")
                if url: links_to_add[col_idx] = url
            elif val is not None: cell_value = str(val)
            row_to_append.append(cell_value)
        sheet.append(row_to_append)
        for col_idx, url in links_to_add.items():
            cell = sheet.cell(row=sheet.max_row, column=col_idx)
            cell.hyperlink = url
            cell.style = "Hyperlink"

def generate_two_sheet_excel(all_processed_rows, key_map):
    out_wb = Workbook()
    sheet1 = out_wb.active; sheet1.title = "UNAL con IA"
    unal_rows = [row for row in all_processed_rows if row.get(key_map.get("menciones")) == "Universidad Nacional de Colombia"]
    _append_rows_to_sheet(sheet1, unal_rows, key_map, include_ai_columns=True)
    sheet2 = out_wb.create_sheet("Todas las Marcas")
    _append_rows_to_sheet(sheet2, all_processed_rows, key_map, include_ai_columns=False)
    output = io.BytesIO(); out_wb.save(output)
    return output.getvalue()

# ======================================
# Proceso Principal y UI
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file, brand_name):
    try:
        openai.api_key = st.secrets["OPENAI_API_KEY"]; openai.aiosession.set(None)
    except Exception:
        st.error("❌ Error: OPENAI_API_KEY no encontrado."); st.stop()

    with st.status("📋 **Paso 1/3:** Limpieza, duplicados y mapeos...", expanded=True) as s:
        all_processed_rows, key_map = run_base_logic(load_workbook(dossier_file, data_only=True).active)
        all_processed_rows = process_mappings_and_links(all_processed_rows, key_map, region_file, internet_file)
        s.update(label="✅ **Paso 1/3:** Base de datos preparada", state="complete")
    
    df_all = pd.DataFrame(all_processed_rows)
    df_unal_to_analyze = df_all[(~df_all["is_duplicate"]) & (df_all[key_map["menciones"]] == brand_name)].copy()

    if df_unal_to_analyze.empty:
        st.warning(f"No se encontraron noticias únicas para '{brand_name}' para analizar con IA.")
    else:
        with st.status(f"🧠 **Paso 2/3:** Analizando Tono y Tema para {len(df_unal_to_analyze)} noticias de '{brand_name}'...", expanded=True) as s:
            p_bar = st.progress(0, text="Iniciando análisis individual de noticias...")
            st.info(f"**Nuevo Enfoque:** Cada una de las {len(df_unal_to_analyze)} noticias será analizada individualmente para máxima precisión. Este proceso puede tardar más.")
            
            textos_a_analizar = [f"Título: {clean_title_for_output(row[key_map['titulo']])}\n\nResumen: {corregir_texto(row[key_map['resumen']])}" for index, row in df_unal_to_analyze.iterrows()]
            
            clasificador = ClasificadorIA(brand_name)
            semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
            tasks = [clasificador.analizar_noticia_individual(texto, semaphore) for texto in textos_a_analizar]
            
            resultados, total_tasks = [], len(tasks)
            for i, f in enumerate(asyncio.as_completed(tasks), 1):
                resultados.append(await f); p_bar.progress(i / total_tasks, text=f"Analizando noticia {i}/{total_tasks}")
            
            df_unal_to_analyze['tono_ai'] = [res['tono'] for res in resultados]
            temas_especificos = [res['tema_especifico'] for res in resultados]
            df_unal_to_analyze['tema_especifico'] = temas_especificos
            
            temas_generales = consolidar_temas_generales(temas_especificos, p_bar)
            df_unal_to_analyze['tema_general'] = temas_generales

            # Mapear nombres de columnas a los normalizados para la actualización
            df_unal_to_analyze.rename(columns={'tono_ai': key_map["tonoai"], 'tema_especifico': key_map["temaespecifico"], 'tema_general': key_map["temageneral"]}, inplace=True)
            
            # Actualizar el DataFrame principal
            df_all.set_index('original_index', inplace=True)
            df_unal_to_analyze.set_index('original_index', inplace=True)
            df_all.update(df_unal_to_analyze[[key_map["tonoai"], key_map["temaespecifico"], key_map["temageneral"]]])
            df_all.reset_index(inplace=True)
            all_processed_rows = df_all.to_dict('records')
        s.update(label="✅ **Paso 2/3:** Análisis con IA completado", state="complete")

    with st.status("📊 **Paso 3/3:** Generando informe final...", expanded=True) as s:
        st.session_state["output_data"] = generate_two_sheet_excel(all_processed_rows, key_map)
        st.session_state["output_filename"] = f"Informe_Analisis_UNAL_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.session_state["processing_complete"] = True
        s.update(label="✅ **Paso 3/3:** Informe generado", state="complete")

def main():
    load_custom_css()
    if not check_password(): return

    st.markdown('<div class="main-header">🎓 Sistema de Análisis de Noticias para la Universidad Nacional</div>', unsafe_allow_html=True)
    st.markdown("Esta herramienta procesa su dossier para deduplicar menciones y aplicar análisis de Tono y Tema con IA **exclusivamente a la marca 'Universidad Nacional de Colombia'**.")

    if not st.session_state.get("processing_complete", False):
        with st.form("input_form"):
            st.markdown("### 📂 Archivos de Entrada")
            col1, col2, col3 = st.columns(3)
            dossier_file = col1.file_uploader("**1. Dossier Principal** (.xlsx)", type=["xlsx"])
            region_file = col2.file_uploader("**2. Mapeo de Región** (.xlsx)", type=["xlsx"])
            internet_file = col3.file_uploader("**3. Mapeo Internet** (.xlsx)", type=["xlsx"])
            st.info("El análisis de IA se ejecutará para la marca **'Universidad Nacional de Colombia'** con el modelo `gpt-4o` para máxima precisión.")

            if st.form_submit_button("🚀 **INICIAR ANÁLISIS COMPLETO (MÁXIMA PRECISIÓN)**", use_container_width=True, type="primary"):
                if not all([dossier_file, region_file, internet_file]):
                    st.error("❌ Faltan archivos obligatorios.")
                else:
                    brand_name = "Universidad Nacional de Colombia"
                    asyncio.run(run_full_process_async(dossier_file, region_file, internet_file, brand_name))
                    st.rerun()
    else:
        st.success("## 🎉 Análisis Completado Exitosamente")
        st.markdown("El informe se ha generado con dos pestañas: **'UNAL con IA'** (con temas específicos y generales) y **'Todas las Marcas'**.")
        st.download_button(label="📥 **DESCARGAR INFORME**", data=st.session_state.output_data, file_name=st.session_state.output_filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")
        if st.button("🔄 **Realizar un Nuevo Análisis**", use_container_width=True):
            pwd = st.session_state.get("password_correct")
            st.session_state.clear()
            st.session_state.password_correct = pwd
            st.rerun()

    st.markdown("<hr><div style='text-align:center;color:#666;font-size:0.9rem;'><p>Sistema de Análisis de Noticias v9.0 (Enfoque de Máxima Precisión) | Adaptado para la Universidad Nacional</p></div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()

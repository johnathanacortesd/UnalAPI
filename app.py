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
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import AgglomerativeClustering
import json
import asyncio
import hashlib
from typing import List, Dict, Tuple, Optional, Any
import gc     # Importación para el recolector de basura

# ======================================
# Configuracion general
# ======================================
st.set_page_config(
    page_title="Análisis de Noticias para la Universidad Nacional",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Modelos y parámetros de la IA
OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14"

# Parámetros de rendimiento y similitud
CONCURRENT_REQUESTS = 40
SIMILARITY_THRESHOLD_TONO = 0.92
SIMILARITY_THRESHOLD_TEMAS = 0.85
SIMILARITY_THRESHOLD_TITULOS = 0.95
MAX_TOKENS_PROMPT_TXT = 4000
WINDOW = 80
NUM_TEMAS_PRINCIPALES = 30

# ======================================
# Estilos CSS (Personalizados para la UNAL)
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
# Autenticacion y Utilidades
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
    max_retries = 3; delay = 1
    for attempt in range(max_retries):
        try: return await api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            await asyncio.sleep(delay); delay *= 2

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
    tema = tema.strip().strip('"').strip("'").strip()
    if tema: tema = tema[0].upper() + tema[1:]
    invalid_words = ["en","de","del","la","el","y","o","con","sin","por","para","sobre"]
    palabras = tema.split()
    while palabras and palabras[-1].lower() in invalid_words: palabras.pop()
    tema = " ".join(palabras)
    if len(tema.split()) > 6: tema = " ".join(tema.split()[:6])
    return tema if tema else "Sin tema"

def extract_link(cell):
    if hasattr(cell, "hyperlink") and cell.hyperlink:
        return {"value": "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        match = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if match: return {"value": "Link", "url": match.group(1)}
    return {"value": cell.value, "url": None}

def normalize_title_for_comparison(title: Any) -> str:
    if not isinstance(title, str): return ""
    tmp = re.split(r"\s*[:|-]\s*", title, 1)
    cleaned = tmp[0] if tmp else title
    return re.sub(r"\W+", " ", cleaned).lower().strip()

def clean_title_for_output(title: Any) -> str:
    """Limpia el título eliminando sufijos comunes como '| El Tiempo'."""
    if not isinstance(title, str):
        return str(title if title is not None else "")
    return re.sub(r"\s*\|\s*[\w\s]+$", "", title).strip()

def corregir_texto(text: Any) -> Any:
    """Limpia el texto del resumen, eliminando etiquetas HTML, placeholders y espacios extra."""
    if not isinstance(text, str):
        return text if text is not None else ""
    
    # Reemplazar secuencias de <br>, <br/>, [...] por un solo espacio
    text = re.sub(r'(<br\s*/?>|\[\.\.\.\])+', ' ', text)
    
    # Colapsar cualquier espacio en blanco múltiple a un solo espacio y limpiar los extremos
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Asegurarse de que el texto útil comience (elimina caracteres basura al inicio)
    match = re.search(r"[A-ZÁÉÍÓÚÑ]", text) # Busca la primera mayúscula
    if match:
        text = text[match.start():]
        
    # Añadir puntos suspensivos para indicar que es un resumen, si no tiene puntuación final
    if text and not text.endswith(('.', '...', '?', '!')):
        text = text + "..."
        
    return text

def normalizar_tipo_medio(tipo_raw: str) -> str:
    if not isinstance(tipo_raw, str): return str(tipo_raw)
    t = unidecode(tipo_raw.strip().lower())
    mapping = {
        "fm": "Radio", "am": "Radio", "radio": "Radio",
        "aire": "Televisión", "cable": "Televisión", "tv": "Televisión", "television": "Televisión", "televisión": "Televisión", "senal abierta": "Televisión", "señal abierta": "Televisión",
        "diario": "Prensa", "prensa": "Prensa",
        "revista": "Revista", "revistas": "Revista",
        "online": "Internet", "internet": "Internet", "digital": "Internet", "web": "Internet"
    }
    default_value = str(tipo_raw).strip().title() if str(tipo_raw).strip() else "Otro"
    return mapping.get(t, default_value)

@st.cache_data(ttl=3600)
def get_embedding(texto: str) -> Optional[List[float]]:
    if not texto: return None
    try:
        resp = call_with_retries(openai.Embedding.create, input=[texto[:2000]], model=OPENAI_MODEL_EMBEDDING)
        return resp["data"][0]["embedding"]
    except Exception: return None

# ======================================
# Agrupacion de textos
# ======================================
def agrupar_textos_similares(textos: List[str], umbral_similitud: float) -> Dict[int, List[int]]:
    if not textos: return {}
    embs = [get_embedding(t) for t in textos]
    valid_indices = [i for i, e in enumerate(embs) if e is not None]
    if len(valid_indices) < 2: return {}
    emb_matrix = np.array([embs[i] for i in valid_indices])
    clustering = AgglomerativeClustering(n_clusters=None, distance_threshold=1 - umbral_similitud, metric="cosine", linkage="average").fit(emb_matrix)
    grupos = defaultdict(list)
    for i, label in enumerate(clustering.labels_): grupos[label].append(valid_indices[i])
    return {gid: g for gid, g in enumerate(grupos.values())}

def seleccionar_representante(indices: List[int], textos: List[str]) -> Tuple[int, str]:
    emb_list, valid = [], []
    for i in indices:
        e = get_embedding(textos[i])
        if e is not None: emb_list.append(e); valid.append(i)
    if not emb_list: return indices[0], textos[indices[0]]
    M = np.array(emb_list)
    centro = M.mean(axis=0, keepdims=True)
    sims = cosine_similarity(M, centro).reshape(-1)
    idx = valid[int(np.argmax(sims))]
    return idx, textos[idx]

# ======================================
# Análisis de tono y tema con IA
# ======================================
class ClasificadorIA:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []

    async def _analizar_grupo_async(self, texto_representante: str, semaphore: asyncio.Semaphore) -> Dict[str, str]:
        async with semaphore:
            aliases_str = ", ".join(self.aliases) if self.aliases else "ninguno"
            # --- MEJORA: Prompt avanzado con técnica "Chain of Thought" ---
            prompt = f"""
            Analiza el siguiente texto de una noticia sobre la marca '{self.marca}' (que también puede aparecer como {aliases_str}). Sigue estos pasos de razonamiento de forma estricta:

            1.  **Identificación del Foco Principal:** Lee el título y el resumen para entender el evento, persona o concepto central que involucra a la marca.
            2.  **Extracción de Palabras Clave:** Extrae de 3 a 5 palabras o frases clave DIRECTAMENTE DEL TEXTO que capturen la esencia de la noticia. No inventes conceptos que no estén escritos.
            3.  **Síntesis del Tema:** Basándote EXCLUSIVAMENTE en las palabras clave extraídas, sintetiza un tema descriptivo y conciso de 2 a 5 palabras. El tema debe ser un resumen lógico del foco principal. No incluyas el nombre de la marca, ciudades, regiones o gentilicios colombianos.
            4.  **Análisis de Tono:** Evalúa el sentimiento general del texto hacia la marca. Clasifícalo como Positivo, Negativo o Neutro. Un tono es Negativo solo si hay una crítica directa, un problema o una controversia explícita. De lo contrario, si solo se informa un hecho, es Neutro.

            **Texto de la Noticia:**
            ---
            {texto_representante[:MAX_TOKENS_PROMPT_TXT]}
            ---

            **Respuesta Final (solo el JSON):**
            Proporciona tu respuesta final en formato JSON, con las claves "tono" y "tema".

            {{
              "tono": "...",
              "tema": "..."
            }}
            """
            try:
                resp = await acall_with_retries(
                    openai.ChatCompletion.acreate,
                    model=OPENAI_MODEL_CLASIFICACION,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=80,
                    temperature=0.0,  # --- MEJORA: Temperatura en 0.0 para máxima objetividad
                    response_format={"type": "json_object"}
                )
                data = json.loads(resp.choices[0].message.content.strip())
                tono = str(data.get("tono", "Neutro")).title()
                tema = limpiar_tema(data.get("tema", "Sin tema"))
                return {"tono": tono if tono in ["Positivo", "Negativo", "Neutro"] else "Neutro", "tema": tema}
            except Exception:
                return {"tono": "Neutro", "tema": "Fallo de Análisis"}

def consolidar_temas(temas: List[str], p_bar) -> List[str]:
    p_bar.progress(0.7, text=f"📊 Consolidando {len(set(temas))} temas únicos en {NUM_TEMAS_PRINCIPALES} grupos principales...")
    tema_counts = Counter(t for t in temas if t != "Sin tema")
    
    temas_a_clusterizar = [t for t, count in tema_counts.items() if count > 1]
    singletons = {t for t, count in tema_counts.items() if count == 1}
    mapa_tema_a_consolidado = {t: t for t in singletons}
    mapa_tema_a_consolidado["Sin tema"] = "Sin tema"

    if not temas_a_clusterizar or len(temas_a_clusterizar) <= NUM_TEMAS_PRINCIPALES:
        p_bar.progress(1.0, "ℹ️ No se requiere consolidación adicional. Usando temas específicos.")
        for t in temas_a_clusterizar: mapa_tema_a_consolidado[t] = t
        return [mapa_tema_a_consolidado.get(t, t) for t in temas]

    emb_temas = {t: get_embedding(t) for t in temas_a_clusterizar}
    temas_validos = [t for t, emb in emb_temas.items() if emb is not None]
    if len(temas_validos) <= NUM_TEMAS_PRINCIPALES:
        p_bar.progress(1.0, "ℹ️ No hay suficientes temas para una consolidación significativa.")
        for t in temas_a_clusterizar: mapa_tema_a_consolidado[t] = t
        return [mapa_tema_a_consolidado.get(t, t) for t in temas]

    emb_matrix = np.array([emb_temas[t] for t in temas_validos])
    n_clusters = min(NUM_TEMAS_PRINCIPALES, len(temas_validos))
    clustering = AgglomerativeClustering(n_clusters=n_clusters, metric="cosine", linkage="average").fit(emb_matrix)
    
    mapa_cluster_a_temas = defaultdict(list)
    for i, label in enumerate(clustering.labels_):
        mapa_cluster_a_temas[label].append(temas_validos[i])

    p_bar.progress(0.85, "🧠 Generando nombres para los temas consolidados...")
    # --- MEJORA: Lógica de consolidación por centralidad semántica ---
    for cluster_id, lista_temas in mapa_cluster_a_temas.items():
        if not lista_temas: continue
        
        if len(lista_temas) == 1:
            tema_principal = lista_temas[0]
        else:
            emb_list = [emb_temas.get(t) for t in lista_temas if emb_temas.get(t) is not None]
            
            if not emb_list:
                tema_principal = max(lista_temas, key=len) # Fallback
            else:
                M = np.array(emb_list)
                centro = M.mean(axis=0, keepdims=True)
                sims = cosine_similarity(M, centro).reshape(-1)
                # Elige el tema del cluster más cercano al centroide semántico
                tema_principal = lista_temas[int(np.argmax(sims))]
        
        for tema in lista_temas:
            mapa_tema_a_consolidado[tema] = tema_principal
    
    p_bar.progress(1.0, "✅ Consolidación de temas completada.")
    return [mapa_tema_a_consolidado.get(t, t) for t in temas]


# ======================================
# Lógica de Duplicados y Procesamiento Base
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
                t1 = normalize_title_for_comparison(processed_rows[idx1].get(key_map.get("titulo")))
                t2 = normalize_title_for_comparison(processed_rows[idx2].get(key_map.get("titulo")))
                if t1 and t2 and SequenceMatcher(None, t1, t2).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                    winner, loser = (idx2, idx1) if len(t1) < len(t2) else (idx1, idx2)
                    processed_rows[loser]["is_duplicate"], processed_rows[loser]["idduplicada"] = True, processed_rows[winner].get(key_map.get("idnoticia"), "")
    return processed_rows

def run_base_logic(sheet):
    headers = [c.value for c in sheet[1] if c.value]
    norm_keys = [norm_key(h) for h in headers]
    key_map = {nk: nk for nk in norm_keys}
    key_map.update({ "titulo": norm_key("Titulo"), "resumen": norm_key("Resumen - Aclaracion"), "menciones": norm_key("Menciones - Empresa"), "medio": norm_key("Medio"), "tonoai": norm_key("Tono AI"), "justificaciontono": norm_key("Justificacion Tono"), "tema": norm_key("Tema"), "idnoticia": norm_key("ID Noticia"), "idduplicada": norm_key("ID duplicada"), "tipodemedio": norm_key("Tipo de Medio"), "hora": norm_key("Hora"), "link_nota": norm_key("Link Nota"), "link_streaming": norm_key("Link (Streaming - Imagen)"), "region": norm_key("Region") })
    rows = [ {norm_keys[i]: c for i, c in enumerate(row) if i < len(norm_keys)} for row in sheet.iter_rows(min_row=2) if not all(c.value is None for c in row) ]
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
        if row["is_duplicate"]: row.update({key_map["tonoai"]: "Duplicada", key_map["tema"]: "Duplicada", key_map["justificaciontono"]: "Noticia duplicada."})
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

# ======================================
# Generación de Excel con dos pestañas
# ======================================
def _append_rows_to_sheet(sheet, rows_data, key_map, include_ai_columns):
    base_order = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Seccion - Programa","Region","Titulo","Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Tier","Audiencia","Tono","Resumen - Aclaracion","Link Nota","Link (Streaming - Imagen)","Menciones - Empresa","ID duplicada"]
    ai_order = ["Tono AI", "Tema"]
    final_order = base_order[:16] + ai_order + base_order[16:] if include_ai_columns else base_order
    
    sheet.append(final_order)
    numeric_columns = {"ID Noticia", "Nro. Pagina", "Dimension", "Duracion - Nro. Caracteres", "CPE", "Tier", "Audiencia"}
    
    for row_data in rows_data:
        titulo_key = key_map.get("titulo")
        if titulo_key in row_data:
            row_data[titulo_key] = clean_title_for_output(row_data.get(titulo_key))

        resumen_key = key_map.get("resumen")
        if resumen_key in row_data:
            row_data[resumen_key] = corregir_texto(row_data.get(resumen_key))

        row_to_append, links_to_add = [], {}
        for col_idx, header in enumerate(final_order, 1):
            nk_header = norm_key(header)
            val = row_data.get(nk_header)
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
    
    sheet1 = out_wb.active
    sheet1.title = "UNAL con IA"
    unal_rows = [row for row in all_processed_rows if row.get(key_map.get("menciones")) == "Universidad Nacional de Colombia"]
    _append_rows_to_sheet(sheet1, unal_rows, key_map, include_ai_columns=True)
    
    sheet2 = out_wb.create_sheet("Todas las Marcas")
    _append_rows_to_sheet(sheet2, all_processed_rows, key_map, include_ai_columns=False)

    output = io.BytesIO()
    out_wb.save(output)
    return output.getvalue()

# ======================================
# Proceso Principal y UI
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file, brand_name, brand_aliases):
    try:
        openai.api_key = st.secrets["OPENAI_API_KEY"]
        openai.aiosession.set(None)
    except Exception:
        st.error("❌ Error: OPENAI_API_KEY no encontrado en los Secrets de Streamlit.")
        st.stop()

    with st.status("📋 **Paso 1/3:** Limpieza, duplicados y mapeos...", expanded=True) as s:
        all_processed_rows, key_map = run_base_logic(load_workbook(dossier_file, data_only=True).active)
        all_processed_rows = process_mappings_and_links(all_processed_rows, key_map, region_file, internet_file)
        s.update(label="✅ **Paso 1/3:** Base de datos preparada", state="complete")
    
    rows_for_unal_analysis = [
        row for row in all_processed_rows 
        if not row.get("is_duplicate") and row.get(key_map.get("menciones")) == brand_name
    ]

    if not rows_for_unal_analysis:
        st.warning(f"No se encontraron noticias únicas para la marca '{brand_name}' para analizar con IA. El informe se generará sin análisis de Tono/Tema.")
    else:
        with st.status(f"🧠 **Paso 2/3:** Analizando Tono y Tema para {len(rows_for_unal_analysis)} noticias de '{brand_name}'...", expanded=True) as s:
            df_unal = pd.DataFrame(rows_for_unal_analysis)
            
            # --- MEJORA: Crear texto para agrupar ponderando más el título ---
            df_unal["texto_para_agrupar"] = (df_unal[key_map["titulo"]].fillna("").astype(str) + ". ") * 3 + \
                                            df_unal[key_map["resumen"]].fillna("").astype(str)
            textos_para_agrupar = df_unal["texto_para_agrupar"].tolist()

            # Texto para el análisis final de la IA (sin ponderación)
            df_unal["resumen_api"] = df_unal[key_map["titulo"]].fillna("").astype(str) + ". " + \
                                     df_unal[key_map["resumen"]].fillna("").astype(str)
            textos_unal = df_unal["resumen_api"].tolist()
            
            p_bar = st.progress(0, text="Optimizando llamadas a la API...")
            grupos_unal = agrupar_textos_similares(textos_para_agrupar, SIMILARITY_THRESHOLD_TEMAS)
            comp = defaultdict(list); dsu = list(range(len(textos_unal)))
            for _, idxs in grupos_unal.items():
                for j in idxs[1:]: dsu[j] = idxs[0]
            for i in range(len(textos_unal)): comp[dsu[i]].append(i)
            
            st.info(f"💡 Optimización: Se procesarán {len(textos_unal)} noticias en {len(comp)} grupos únicos.")
            
            representantes = {cid: seleccionar_representante(idxs, textos_unal)[1] for cid, idxs in comp.items()}
            clasificador = ClasificadorIA(brand_name, brand_aliases)
            semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
            tasks = [clasificador._analizar_grupo_async(rep_texto, semaphore) for rep_texto in representantes.values()]
            
            resultados_brutos = []
            total_tasks = len(tasks)
            for i, f in enumerate(asyncio.as_completed(tasks), 1):
                resultados_brutos.append(await f)
                p_bar.progress(i / total_tasks, text=f"Analizando grupo {i}/{total_tasks}")
            
            resultados_por_grupo = {list(representantes.keys())[i]: res for i, res in enumerate(resultados_brutos)}
            
            temas_iniciales = [None] * len(textos_unal)
            for cid, idxs in comp.items():
                res = resultados_por_grupo.get(cid, {"tono": "Neutro", "tema": "Sin Análisis"})
                for i in idxs:
                    original_idx = df_unal.index[i]
                    df_unal.loc[original_idx, key_map["tonoai"]] = res["tono"]
                    temas_iniciales[i] = res["tema"]
            
            temas_consolidados = consolidar_temas(temas_iniciales, p_bar)
            df_unal[key_map["tema"]] = temas_consolidados
            
            results_map = df_unal.set_index("original_index").to_dict("index")
            for row in all_processed_rows:
                if row["original_index"] in results_map:
                    row.update(results_map[row["original_index"]])
        s.update(label="✅ **Paso 2/3:** Análisis con IA completado", state="complete")

    with st.status("📊 **Paso 3/3:** Generando informe final con dos pestañas...", expanded=True) as s:
        st.session_state["output_data"] = generate_two_sheet_excel(all_processed_rows, key_map)
        st.session_state["output_filename"] = f"Informe_Analisis_UNAL_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.session_state["processing_complete"] = True
        s.update(label="✅ **Paso 3/3:** Informe generado exitosamente", state="complete")

def main():
    load_custom_css()
    if not check_password(): return

    st.markdown('<div class="main-header">🎓 Sistema de Análisis de Noticias para la Universidad Nacional</div>', unsafe_allow_html=True)
    st.markdown("Esta herramienta procesa su dossier de noticias para deduplicar menciones y aplicar análisis de Tono y Tema con IA **exclusivamente a la marca 'Universidad Nacional de Colombia'**.")

    if not st.session_state.get("processing_complete", False):
        with st.form("input_form"):
            st.markdown("### 📂 Archivos de Entrada")
            col1, col2, col3 = st.columns(3)
            dossier_file = col1.file_uploader("**1. Dossier Principal** (.xlsx)", type=["xlsx"])
            region_file = col2.file_uploader("**2. Mapeo de Región** (.xlsx)", type=["xlsx"])
            internet_file = col3.file_uploader("**3. Mapeo Internet** (.xlsx)", type=["xlsx"])
            
            st.info("El análisis de IA se ejecutará automáticamente para la marca **'Universidad Nacional de Colombia'**.")
            
            brand_aliases_text = st.text_area("**Alias y voceros de la UNAL** (separados por ;)", value="UNAL;UN;U. Nacional;Universidad Nacional", height=80)

            if st.form_submit_button("🚀 **INICIAR ANÁLISIS COMPLETO**", use_container_width=True, type="primary"):
                if not all([dossier_file, region_file, internet_file]):
                    st.error("❌ Faltan archivos obligatorios.")
                else:
                    brand_name = "Universidad Nacional de Colombia"
                    aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                    asyncio.run(run_full_process_async(dossier_file, region_file, internet_file, brand_name, aliases))
                    st.rerun()
    else:
        st.success("## 🎉 Análisis Completado Exitosamente")
        st.markdown("El informe en Excel ha sido generado con dos pestañas: **'UNAL con IA'** y **'Todas las Marcas'**.")
        st.download_button(
            label="📥 **DESCARGAR INFORME**",
            data=st.session_state.output_data,
            file_name=st.session_state.output_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary"
        )
        if st.button("🔄 **Realizar un Nuevo Análisis**", use_container_width=True):
            pwd = st.session_state.get("password_correct")
            st.session_state.clear()
            st.session_state.password_correct = pwd
            st.rerun()

    st.markdown("<hr><div style='text-align:center;color:#666;font-size:0.9rem;'><p>Sistema de Análisis de Noticias v6.2 (Mejorado) | Adaptado para la Universidad Nacional</p></div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()

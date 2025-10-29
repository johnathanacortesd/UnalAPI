# ======================================
# Importaciones
# ======================================
import streamlit as st
import pandas as pd
from openpyxl import load_workbook, Workbook
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
import json
import asyncio
from typing import List, Dict, Tuple

# ### NUEVO: Importaciones para modelos locales ###
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from sentence_transformers import SentenceTransformer
import torch
from sklearn.cluster import KMeans

# ======================================
# Configuracion general
# ======================================
st.set_page_config(
    page_title="Análisis de Noticias para la Universidad Nacional",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ### MODIFICADO: Se mantiene solo modelo de OpenAI para etiquetado de temas ###
OPENAI_MODEL_ETIQUETADO = "gpt-4.1-nano-2025-04-14"

# Marcas objetivo a analizar
TARGET_BRANDS = ["U. Nacional de Colombia", "Universidad Nacional de Colombia"]

# Parámetros
SIMILARITY_THRESHOLD_TITULOS = 0.95
MAX_TOKENS_PROMPT_TXT = 4000
NUM_TEMAS_CLUSTERING = 20  # Número de temas a generar

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
# Autenticacion y Utilidades (TODAS LAS FUNCIONES ORIGINALES RESTAURADAS)
# ======================================
def check_password() -> bool:
    if st.session_state.get("password_correct", False):
        return True
    st.markdown('<div class="main-header">🔐 Portal de Acceso Seguro</div>', unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("password_form"):
            password = st.text_input("🔑 Contraseña:", type="password")
            if st.form_submit_button("🚀 Ingresar", use_container_width=True, type="primary"):
                if password == st.secrets.get("APP_PASSWORD", "INVALID_DEFAULT"):
                    st.session_state["password_correct"] = True
                    st.success("✅ Acceso autorizado.")
                    st.balloons()
                    time.sleep(1.5)
                    st.rerun()
                else:
                    st.error("❌ Contraseña incorrecta")
    return False

async def acall_with_retries(api_func, *args, **kwargs):
    max_retries = 3
    delay = 1
    for attempt in range(max_retries):
        try:
            return await api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            await asyncio.sleep(delay)
            delay *= 2

def call_with_retries(api_func, *args, **kwargs):
    max_retries = 3
    delay = 1
    for attempt in range(max_retries):
        try:
            return api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(delay)
            delay *= 2

def norm_key(text: Any) -> str:
    if text is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

def limpiar_tema(tema: str) -> str:
    if not tema:
        return "Sin tema"
    tema = tema.strip().strip('"').strip("'").strip()
    if tema:
        tema = tema[0].upper() + tema[1:]
    invalid_words = ["en","de","del","la","el","y","o","con","sin","por","para","sobre"]
    palabras = tema.split()
    while palabras and palabras[-1].lower() in invalid_words:
        palabras.pop()
    tema = " ".join(palabras)
    if len(tema.split()) > 6:
        tema = " ".join(tema.split()[:6])
    return tema if tema else "Sin tema"

def extract_link(cell):
    if hasattr(cell, "hyperlink") and cell.hyperlink:
        return {"value": "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        match = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if match:
            return {"value": "Link", "url": match.group(1)}
    return {"value": cell.value, "url": None}

def normalize_title_for_comparison(title: Any) -> str:
    if not isinstance(title, str):
        return ""
    tmp = re.split(r"\s*[:|-]\s*", title, 1)
    cleaned = tmp[0] if tmp else title
    return re.sub(r"\W+", " ", cleaned).lower().strip()

def clean_title_for_output(title: Any) -> str:
    if not isinstance(title, str):
        return str(title if title is not None else "")
    return re.sub(r"\s*\|\s*[\w\s]+$", "", title).strip()

def corregir_texto(text: Any) -> Any:
    if not isinstance(text, str):
        return text if text is not None else ""
    text = re.sub(r'(<br\s*/?>|\[\.\.\.\])+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    match = re.search(r"[A-ZÁÉÍÓÚÑ]", text)
    if match:
        text = text[match.start():]
    if text and not text.endswith(('.', '...', '?', '!')):
        text = text + "..."
    return text

def normalizar_tipo_medio(tipo_raw: str) -> str:
    if not isinstance(tipo_raw, str):
        return str(tipo_raw)
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

# ======================================
# ### NUEVO: Carga de Modelos Locales con Caché ###
# ======================================
@st.cache_resource
def cargar_modelo_sentimiento():
    """Carga el modelo y tokenizador de sentimiento de Hugging Face."""
    tokenizer = AutoTokenizer.from_pretrained("clapAI/roberta-large-multilingual-sentiment")
    model = AutoModelForSequenceClassification.from_pretrained("clapAI/roberta-large-multilingual-sentiment")
    return tokenizer, model

@st.cache_resource
def cargar_modelo_embeddings():
    """Carga el modelo de embeddings de SentenceTransformers."""
    model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")
    return model

def analizar_tono_local(texto: str, tokenizer, model) -> str:
    """Analiza el tono de un texto usando un modelo local."""
    if not texto or not isinstance(texto, str):
        return "Neutro"
    
    try:
        inputs = tokenizer(texto, return_tensors="pt", truncation=True, max_length=512)
        with torch.no_grad():
            logits = model(**inputs).logits
        
        # Mapeo de la salida del modelo a nuestras etiquetas
        label_map = { "negative": "Negativo", "neutral": "Neutro", "positive": "Positivo" }
        
        predicted_class_id = torch.argmax(logits, dim=-1).item()
        predicted_label = model.config.id2label[predicted_class_id]
        
        return label_map.get(predicted_label, "Neutro")
        
    except Exception:
        return "Neutro" # Fallback

# ======================================
# ### MODIFICADO: Lógica de Análisis de Tono y Tema ###
# ======================================
async def _etiquetar_cluster_con_ia(texto_representante: str) -> str:
    """Función auxiliar que llama a OpenAI SÓLO para generar la etiqueta de un cluster."""
    prompt = f"""
    Eres un analista de medios experto en sintetizar información.
    Basado en la siguiente noticia, que es la más representativa de un grupo de noticias similares, crea un tema corto y descriptivo de 3 a 5 palabras.
    El tema debe ser claro, conciso y capturar la esencia del evento principal.

    Ejemplo:
    - Noticia: "El rector de la UNAL, Ismael Peña, se posesionó en una notaría. Hubo protestas de estudiantes en el campus."
    - Tema generado: "Posesión del rector y protestas"

    Noticia a analizar:
    ---
    {texto_representante[:MAX_TOKENS_PROMPT_TXT]}
    ---

    Genera únicamente el tema, sin explicaciones adicionales.
    """
    try:
        resp = await acall_with_retries(
            openai.ChatCompletion.acreate,
            model=OPENAI_MODEL_ETIQUETADO,
            messages=[
                {"role": "system", "content": "Generas temas cortos y descriptivos para grupos de noticias."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=20,
            temperature=0.1,
        )
        tema = resp.choices[0].message.content.strip().strip('"')
        return limpiar_tema(tema)
    except Exception:
        return "Tema no disponible"

async def generar_y_etiquetar_temas_local(noticias: List[Dict], key_map: Dict[str, str], p_bar) -> Dict[int, str]:
    """
    Genera embeddings, agrupa en clusters y luego etiqueta cada cluster para definir los temas.
    Devuelve un mapeo de original_index -> tema_asignado.
    """
    p_bar.progress(0.1, text="🧠 Generando embeddings con modelo local...")
    modelo_emb = cargar_modelo_embeddings()
    
    textos_para_embed = [
        corregir_texto(n.get(key_map.get("titulo"), "")) + ". " + corregir_texto(n.get(key_map.get("resumen"), ""))
        for n in noticias
    ]
    
    embeddings = modelo_emb.encode(textos_para_embed, show_progress_bar=False, batch_size=32)
    
    p_bar.progress(0.5, f"🔄 Agrupando noticias en {NUM_TEMAS_CLUSTERING} temas...")
    
    kmeans = KMeans(n_clusters=NUM_TEMAS_CLUSTERING, random_state=42, n_init='auto')
    kmeans.fit(embeddings)
    
    for i, noticia in enumerate(noticias):
        noticia['cluster_id'] = kmeans.labels_[i]

    p_bar.progress(0.7, f"✍️ Etiquetando los {NUM_TEMAS_CLUSTERING} temas con IA...")
    
    mapa_cluster_a_tema = {}
    tasks = []
    
    for cluster_id in range(NUM_TEMAS_CLUSTERING):
        indices_cluster = [i for i, n in enumerate(noticias) if n['cluster_id'] == cluster_id]
        if not indices_cluster:
            mapa_cluster_a_tema[cluster_id] = "Noticias sin tema claro"
            continue
            
        embeddings_cluster = embeddings[indices_cluster]
        centroide = kmeans.cluster_centers_[cluster_id]
        distancias = np.linalg.norm(embeddings_cluster - centroide, axis=1)
        indice_representante_local = np.argmin(distancias)
        indice_representante_global = indices_cluster[indice_representante_local]
        
        texto_rep = textos_para_embed[indice_representante_global]
        tasks.append(_etiquetar_cluster_con_ia(texto_rep))

    etiquetas_temas = await asyncio.gather(*tasks)
    
    for i, tema in enumerate(etiquetas_temas):
        mapa_cluster_a_tema[i] = tema
        
    p_bar.progress(0.9, "✅ Etiquetado completado. Asignando temas...")

    mapa_final_temas = {}
    for noticia in noticias:
        idx = noticia['original_index']
        cluster = noticia['cluster_id']
        mapa_final_temas[idx] = mapa_cluster_a_tema.get(cluster, "Tema no asignado")
        
    return mapa_final_temas

# ======================================
# Lógica de Duplicados y Procesamiento Base (SIN CAMBIOS)
# ======================================
def detectar_duplicados_avanzado(rows: List[Dict], key_map: Dict[str, str]) -> List[Dict]:
    processed_rows = deepcopy(rows)
    seen_online_url, seen_broadcast, online_title_buckets = {}, {}, defaultdict(list)
    for i, row in enumerate(processed_rows):
        if row.get("is_duplicate"):
            continue
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
                else:
                    seen_online_url[key] = i
            if medio_norm and mencion_norm:
                online_title_buckets[(medio_norm, mencion_norm)].append(i)
        elif tipo_medio in ["Radio", "Televisión"]:
            hora = str(row.get(key_map.get("hora"), "")).strip()
            if mencion_norm and medio_norm and hora:
                key = (mencion_norm, medio_norm, hora)
                if key in seen_broadcast:
                    row["is_duplicate"], row["idduplicada"] = True, processed_rows[seen_broadcast[key]].get(key_map.get("idnoticia"), "")
                else:
                    seen_broadcast[key] = i
    for _, indices in online_title_buckets.items():
        if len(indices) < 2:
            continue
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                idx1, idx2 = indices[i], indices[j]
                if processed_rows[idx1].get("is_duplicate") or processed_rows[idx2].get("is_duplicate"):
                    continue
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
    key_map.update({
        "titulo": norm_key("Titulo"), "resumen": norm_key("Resumen - Aclaracion"),
        "menciones": norm_key("Menciones - Empresa"), "medio": norm_key("Medio"),
        "tonoai": norm_key("Tono AI"), "justificaciontono": norm_key("Justificacion Tono"),
        "tema": norm_key("Tema"), "idnoticia": norm_key("ID Noticia"),
        "idduplicada": norm_key("ID duplicada"), "tipodemedio": norm_key("Tipo de Medio"),
        "hora": norm_key("Hora"), "link_nota": norm_key("Link Nota"),
        "link_streaming": norm_key("Link (Streaming - Imagen)"), "region": norm_key("Region")
    })
    rows_data = sheet.iter_rows(min_row=2)
    rows = [{norm_keys[i]: cell for i, cell in enumerate(row) if i < len(norm_keys)} for row in rows_data if not all(c.value is None for c in row)]
    
    split_rows = []
    for r_cells in rows:
        base = {k: extract_link(v) if k in [key_map["link_nota"], key_map["link_streaming"]] else v.value for k, v in r_cells.items()}
        base[key_map["tipodemedio"]] = normalizar_tipo_medio(base.get(key_map["tipodemedio"]))
        m_list = [m.strip() for m in str(base.get(key_map["menciones"], "")).split(";") if m.strip()]
        for m in m_list or [base.get(key_map["menciones"])]:
            new = deepcopy(base)
            new[key_map["menciones"]] = m
            split_rows.append(new)
    
    for idx, row in enumerate(split_rows):
        row.update({"original_index": idx, "is_duplicate": False})
    
    processed_rows = detectar_duplicados_avanzado(split_rows, key_map)
    for row in processed_rows:
        if row["is_duplicate"]:
            row.update({
                key_map["tonoai"]: "Duplicada", key_map["tema"]: "Duplicada",
                key_map["justificaciontono"]: "Noticia duplicada."
            })
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

def process_sov_mapping_final(all_rows: List[Dict], key_map: Dict[str, str], sov_file) -> List[Dict]:
    try:
        df_sov = pd.read_excel(sov_file)
        if df_sov.empty: return all_rows
        cols_by_norm = {norm_key(c): c for c in df_sov.columns}
        menc_col = cols_by_norm.get(norm_key("Menciones - Empresa"))
        name_col = cols_by_norm.get(norm_key("Nombre"))
        if not menc_col or not name_col:
            st.warning("⚠️ El archivo SOV no contiene las columnas esperadas.")
            return all_rows

        sov_map = {str(row.get(menc_col, "")).strip().lower(): str(row.get(name_col)).strip() for _, row in df_sov.iterrows() if str(row.get(menc_col,"")).strip() and str(row.get(name_col,"")).strip()}

        if not sov_map: return all_rows

        menc_key = key_map.get("menciones")
        for r in all_rows:
            mk = str(r.get(menc_key, "")).strip().lower()
            if mk in sov_map: r[menc_key] = sov_map[mk]
        return all_rows
    except Exception as e:
        st.warning(f"⚠️ No se pudo aplicar el mapeo SOV: {e}")
        return all_rows

# ======================================
# Generación de Excel (SIN CAMBIOS)
# ======================================
def _append_rows_to_sheet(sheet, rows_data, key_map, include_ai_columns):
    base_order = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Seccion - Programa","Region","Titulo","Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Tier","Audiencia","Tono","Resumen - Aclaracion","Link Nota","Link (Streaming - Imagen)","Menciones - Empresa","ID duplicada"]
    ai_order = ["Tono AI", "Tema"]
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
    unal_rows = [row for row in all_processed_rows if row.get("__is_target_brand")]
    _append_rows_to_sheet(sheet1, unal_rows, key_map, include_ai_columns=True)
    sheet2 = out_wb.create_sheet("Todas las Marcas")
    _append_rows_to_sheet(sheet2, all_processed_rows, key_map, include_ai_columns=False)
    output = io.BytesIO(); out_wb.save(output)
    return output.getvalue()

# ======================================
# ### MODIFICADO: Proceso Principal y UI ###
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file, sov_file):
    try:
        openai.api_key = st.secrets["OPENAI_API_KEY"]
        openai.aiosession.set(None)
    except Exception:
        st.error("❌ Error: OPENAI_API_KEY no encontrado. Es necesario para el etiquetado de temas.")
        st.stop()

    with st.status("📋 **Paso 1/3:** Limpieza, duplicados y mapeos...", expanded=True) as s:
        all_processed_rows, key_map = run_base_logic(load_workbook(dossier_file, data_only=True).active)
        all_processed_rows = process_mappings_and_links(all_processed_rows, key_map, region_file, internet_file)
        s.update(label="✅ **Paso 1/3:** Base de datos preparada", state="complete")
    
    for row in all_processed_rows:
        row["__is_target_brand"] = (row.get(key_map.get("menciones")) in TARGET_BRANDS)

    index_to_row_map = {row['original_index']: row for row in all_processed_rows}
    target_rows_all = [row for row in all_processed_rows if row.get("__is_target_brand") and not row.get("is_duplicate")]

    if not target_rows_all:
        st.warning("No se encontraron noticias únicas para las marcas objetivo para analizar.")
    else:
        with st.status("🧠 **Paso 2/3:** Analizando Tono y Tema con modelos locales...", expanded=True) as s:
            p_bar = st.progress(0, text="🚀 Preparando modelos locales...")
            
            tokenizer_sent, model_sent = cargar_modelo_sentimiento()
            
            p_bar.progress(0.05, text=f"📊 Analizando tono para {len(target_rows_all)} noticias...")
            for i, row in enumerate(target_rows_all):
                texto = corregir_texto(row.get(key_map.get("titulo"), "")) + ". " + corregir_texto(row.get(key_map.get("resumen"), ""))
                tono = analizar_tono_local(texto, tokenizer_sent, model_sent)
                index_to_row_map[row['original_index']][key_map.get("tonoai")] = tono
                if (i + 1) % 50 == 0:
                    p_bar.progress(0.05 + (i / len(target_rows_all)) * 0.05, text=f"📊 Analizando tono... {i+1}/{len(target_rows_all)}")

            mapa_idx_a_tema = await generar_y_etiquetar_temas_local(target_rows_all, key_map, p_bar)
            
            for idx, tema in mapa_idx_a_tema.items():
                if idx in index_to_row_map:
                    index_to_row_map[idx][key_map.get("tema")] = tema

            s.update(label="✅ **Paso 2/3:** Análisis local completado", state="complete")

    with st.status("📊 **Paso 3/3:** Aplicando SOV y generando informe final...", expanded=True) as s:
        final_processed_rows = list(index_to_row_map.values())
        final_processed_rows = process_sov_mapping_final(final_processed_rows, key_map, sov_file)
        st.session_state["output_data"] = generate_two_sheet_excel(final_processed_rows, key_map)
        st.session_state["output_filename"] = f"Informe_Analisis_UNAL_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.session_state["processing_complete"] = True
        s.update(label="✅ **Paso 3/3:** Informe generado exitosamente", state="complete")

def main():
    load_custom_css()
    if not check_password():
        return

    st.markdown('<div class="main-header">🎓 Sistema de Análisis de Noticias para la Universidad Nacional</div>', unsafe_allow_html=True)
    st.markdown(
        "Esta herramienta utiliza **modelos de IA locales** para analizar Tono y Tema en las noticias de 'U. Nacional de Colombia' y 'Universidad Nacional de Colombia'. "
        f"Los temas se generan agrupando noticias similares en **{NUM_TEMAS_CLUSTERING} categorías principales**."
    )

    if not st.session_state.get("processing_complete", False):
        with st.form("input_form"):
            st.markdown("### 📂 Archivos de Entrada")
            col1, col2, col3, col4 = st.columns(4)
            dossier_file = col1.file_uploader("**1. Dossier Principal** (.xlsx)", type=["xlsx"])
            region_file = col2.file_uploader("**2. Mapeo de Región** (.xlsx)", type=["xlsx"])
            internet_file = col3.file_uploader("**3. Mapeo Internet** (.xlsx)", type=["xlsx"])
            sov_file = col4.file_uploader("**4. Mapeo SOV** (.xlsx)", type=["xlsx"])

            st.info("El análisis de IA se ejecutará automáticamente solo para 'U. Nacional de Colombia' y 'Universidad Nacional de Colombia'.")

            if st.form_submit_button("🚀 **INICIAR ANÁLISIS COMPLETO**", use_container_width=True, type="primary"):
                if not all([dossier_file, region_file, internet_file, sov_file]):
                    st.error("❌ Faltan archivos obligatorios.")
                else:
                    asyncio.run(run_full_process_async(dossier_file, region_file, internet_file, sov_file))
                    st.rerun()
    else:
        st.success("## 🎉 Análisis Completado Exitosamente")
        st.markdown("El informe en Excel ha sido generado con dos pestañas: **'UNAL con IA'** (solo marcas objetivo) y **'Todas las Marcas'**.")
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

    st.markdown("<hr><div style='text-align:center;color:#666;font-size:0.9rem;'><p>Sistema de Análisis de Noticias v8.1.0 (Local Models Edition) | Adaptado para la Universidad Nacional</p></div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()

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
import re
import time
from unidecode import unidecode
import numpy as np
import json
from typing import List, Dict, Tuple, Any

# ### LIBRERÍAS PARA MODELOS LOCALES ###
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from sentence_transformers import SentenceTransformer
import torch
from sklearn.cluster import KMeans
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
# Modelo de sentimiento local (ligero y multilingüe)
MODELO_SENTIMIENTO_LOCAL = "nlptown/bert-base-multilingual-uncased-sentiment"
# Modelo de embeddings local
MODELO_EMBEDDINGS_LOCAL = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"

# Marcas objetivo a analizar
TARGET_BRANDS = ["U. Nacional de Colombia", "Universidad Nacional de Colombia"]

# Parámetros
SIMILARITY_THRESHOLD_TITULOS = 0.90  # Umbral para considerar títulos similares
SIMILARITY_THRESHOLD_RESUMENES = 0.85  # Umbral para considerar resúmenes similares

# 30 TEMAS PREDEFINIDOS (4-6 palabras cada uno)
TEMAS_PREDEFINIDOS = [
    # Gobernanza y Administración
    "Elección y Gestión del Rector",
    "Decisiones del Consejo Superior Universitario",
    "Presupuesto y Financiación Universitaria",
    "Políticas y Reformas Administrativas",
    "Nombramientos y Cargos Directivos",
    # Vida Académica e Investigación
    "Proceso de Admisión y Aspirantes",
    "Desarrollo de Programas Académicos",
    "Investigaciones y Publicaciones Científicas",
    "Rankings y Acreditación Institucional",
    "Grados, Egresados y Ceremonias",
    "Colaboraciones y Convenios Académicos",
    # Vida Estudiantil y Bienestar
    "Protestas y Movilización Estudiantil",
    "Actividades y Grupos Estudiantiles",
    "Bienestar y Apoyo Estudiantil",
    "Asuntos de Representación Estudiantil",
    "Eventos Culturales y Deportivos",
    # Campus, Infraestructura y Seguridad
    "Desarrollo de Infraestructura y Sedes",
    "Seguridad y Orden Público Campus",
    "Sostenibilidad y Medio Ambiente Campus",
    "Conectividad y Recursos Tecnológicos",
    # Relación con la Sociedad y el País
    "Aportes a Políticas Públicas",
    "Proyectos de Extensión y Comunidad",
    "Relación con el Gobierno Nacional",
    "Debates sobre Educación Superior",
    "Alianzas con Sector Privado",
    # Conflictos, Controversias y Logros
    "Controversias y Denuncias Internas",
    "Reconocimientos y Premios Institucionales",
    "Egresados Destacados y Nombramientos",
    "Conflictos Laborales y Profesorado",
    "Relaciones con Egresados Alumni"
]

NUM_TEMAS = len(TEMAS_PREDEFINIDOS)

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
        .timer-box { background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%); padding: 1.5rem; border-radius: 12px; text-align: center; margin: 1rem 0; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .timer-box h2 { color: #01579b; margin: 0; font-size: 2rem; }
        .timer-box p { color: #0277bd; margin: 0.5rem 0 0 0; font-size: 1rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )

# ======================================
# Autenticación y Funciones de Utilidad
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
                    st.success("✅ Acceso autorizado.")
                    st.balloons(); time.sleep(1.5); st.rerun()
                else:
                    st.error("❌ Contraseña incorrecta")
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
    """Normaliza texto para comparación de similitud"""
    if not isinstance(texto, str): return ""
    # Remover puntuación, espacios extra y convertir a minúsculas
    texto = re.sub(r'[^\w\s]', '', unidecode(texto.lower()))
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def calcular_similitud_textos(texto1: str, texto2: str) -> float:
    """Calcula similitud entre dos textos usando SequenceMatcher"""
    if not texto1 or not texto2:
        return 0.0
    norm1 = normalizar_texto_para_comparacion(texto1)
    norm2 = normalizar_texto_para_comparacion(texto2)
    if not norm1 or not norm2:
        return 0.0
    return SequenceMatcher(None, norm1, norm2).ratio()

def agrupar_noticias_similares(noticias: List[Dict], key_map: Dict[str, str]) -> Dict[int, List[int]]:
    """
    Agrupa noticias que tienen títulos o resúmenes similares.
    Retorna un diccionario: {representante_idx: [lista de índices similares]}
    """
    grupos = {}  # {representante_idx: [idx1, idx2, ...]}
    procesados = set()
    
    for i, noticia_i in enumerate(noticias):
        if i in procesados:
            continue
            
        titulo_i = str(noticia_i.get(key_map.get('titulo'), ''))
        resumen_i = str(noticia_i.get(key_map.get('resumen'), ''))
        
        # Esta noticia será el representante de su grupo
        grupo_actual = [i]
        procesados.add(i)
        
        # Buscar noticias similares
        for j, noticia_j in enumerate(noticias):
            if j <= i or j in procesados:
                continue
                
            titulo_j = str(noticia_j.get(key_map.get('titulo'), ''))
            resumen_j = str(noticia_j.get(key_map.get('resumen'), ''))
            
            # Calcular similitud de títulos y resúmenes
            sim_titulo = calcular_similitud_textos(titulo_i, titulo_j)
            sim_resumen = calcular_similitud_textos(resumen_i, resumen_j)
            
            # Si el título O el resumen son muy similares, agregar al grupo
            if sim_titulo >= SIMILARITY_THRESHOLD_TITULOS or sim_resumen >= SIMILARITY_THRESHOLD_RESUMENES:
                grupo_actual.append(j)
                procesados.add(j)
        
        grupos[i] = grupo_actual
    
    return grupos
    if not isinstance(tipo_raw, str): return str(tipo_raw)
    t = unidecode(tipo_raw.strip().lower())
    mapping = {"fm": "Radio", "am": "Radio", "radio": "Radio", "aire": "Televisión", "cable": "Televisión", "tv": "Televisión", "television": "Televisión", "televisión": "Televisión", "senal abierta": "Televisión", "señal abierta": "Televisión", "diario": "Prensa", "prensa": "Prensa", "revista": "Revista", "revistas": "Revista", "online": "Internet", "internet": "Internet", "digital": "Internet", "web": "Internet"}
    return mapping.get(t, str(tipo_raw).strip().title() if str(tipo_raw).strip() else "Otro")

def format_tiempo(segundos: float) -> str:
    """Formatea segundos en formato legible"""
    if segundos < 60:
        return f"{segundos:.1f} segundos"
    elif segundos < 3600:
        minutos = segundos / 60
        return f"{minutos:.1f} minutos"
    else:
        horas = segundos / 3600
        return f"{horas:.2f} horas"

# ======================================
# Carga de Modelos Locales Optimizada
# ======================================
@st.cache_resource
def cargar_modelo_sentimiento():
    """Carga el modelo y tokenizador de sentimiento (versión ligera)."""
    tokenizer = AutoTokenizer.from_pretrained(MODELO_SENTIMIENTO_LOCAL)
    model = AutoModelForSequenceClassification.from_pretrained(MODELO_SENTIMIENTO_LOCAL)
    return tokenizer, model

@st.cache_resource
def cargar_modelo_embeddings():
    """Carga el modelo de embeddings."""
    model = SentenceTransformer(MODELO_EMBEDDINGS_LOCAL)
    return model

@st.cache_resource
def generar_embeddings_temas():
    """Genera embeddings para los temas predefinidos (se hace una sola vez)"""
    modelo = cargar_modelo_embeddings()
    embeddings = modelo.encode(TEMAS_PREDEFINIDOS, show_progress_bar=False)
    return embeddings

# ======================================
# Lógica de Análisis de Tono y Tema con Modelos Locales
# ======================================
def analizar_tono_local(texto: str, tokenizer, model, progress_bar=None, current=0, total=0) -> str:
    """Analiza el tono usando el modelo de 1-5 estrellas y lo mapea a etiquetas."""
    if not texto or not isinstance(texto, str):
        return "Neutro"
    try:
        inputs = tokenizer(texto, return_tensors="pt", truncation=True, max_length=512)
        with torch.no_grad():
            logits = model(**inputs).logits
        
        # El resultado es un índice de 0 a 4 (corresponde a 1 a 5 estrellas)
        score_index = torch.argmax(logits, dim=-1).item()
        
        if score_index <= 1: # 1 o 2 estrellas
            return "Negativo"
        elif score_index == 2: # 3 estrellas
            return "Neutro"
        else: # 4 o 5 estrellas
            return "Positivo"
    except Exception:
        return "Neutro" # Fallback

def asignar_tema_por_similitud(texto_noticia: str, modelo_emb, embeddings_temas) -> str:
    """
    Asigna un tema a la noticia basándose en similitud de coseno
    con los embeddings de los temas predefinidos.
    """
    try:
        embedding_noticia = modelo_emb.encode([texto_noticia], show_progress_bar=False)
        similitudes = cosine_similarity(embedding_noticia, embeddings_temas)[0]
        idx_mas_similar = np.argmax(similitudes)
        return TEMAS_PREDEFINIDOS[idx_mas_similar]
    except Exception:
        return "Tema no asignado"

def generar_temas_sin_api(noticias: List[Dict], key_map: Dict[str, str], p_bar) -> Dict[int, str]:
    """
    Genera embeddings de las noticias y las asigna al tema predefinido más similar.
    No requiere API de OpenAI.
    """
    p_bar.progress(0.1, text="🧠 Cargando modelo de embeddings...")
    modelo_emb = cargar_modelo_embeddings()
    
    p_bar.progress(0.3, text="📚 Preparando temas predefinidos...")
    embeddings_temas = generar_embeddings_temas()
    
    p_bar.progress(0.5, text=f"🔍 Analizando {len(noticias)} noticias...")
    mapa_idx_a_tema = {}
    
    for i, noticia in enumerate(noticias):
        texto = f"{corregir_texto(noticia.get(key_map.get('titulo'), ''))}. {corregir_texto(noticia.get(key_map.get('resumen'), ''))}"
        tema = asignar_tema_por_similitud(texto, modelo_emb, embeddings_temas)
        mapa_idx_a_tema[noticia['original_index']] = tema
        
        if (i + 1) % 10 == 0:  # Actualizar cada 10 noticias
            progreso = 0.5 + (0.4 * (i + 1) / len(noticias))
            p_bar.progress(progreso, text=f"🔍 Procesadas {i+1}/{len(noticias)} noticias...")
    
    p_bar.progress(0.95, "✅ Asignación de temas completada")
    return mapa_idx_a_tema

# ======================================
# Lógica de Procesamiento de Datos (Base)
# ======================================
def detectar_duplicados_avanzado(rows: List[Dict], key_map: Dict[str, str]) -> List[Dict]:
    processed_rows = deepcopy(rows)
    seen_online_url, seen_broadcast, online_title_buckets = {}, {}, defaultdict(list)
    for i, row in enumerate(processed_rows):
        if row.get("is_duplicate"): continue
        tipo_medio, mencion_norm, medio_norm = normalizar_tipo_medio(str(row.get(key_map.get("tipodemedio")))), norm_key(row.get(key_map.get("menciones"))), norm_key(row.get(key_map.get("medio")))
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
    rows = [{norm_keys[i]: cell for i, cell in enumerate(row) if i < len(norm_keys)} for row in sheet.iter_rows(min_row=2) if not all(c.value is None for c in row)]
    split_rows = []
    for r_cells in rows:
        base = {k: extract_link(v) if k in [key_map["link_nota"], key_map["link_streaming"]] else v.value for k, v in r_cells.items()}
        base[key_map["tipodemedio"]] = normalizar_tipo_medio(base.get(key_map["tipodemedio"]))
        for m in [m.strip() for m in str(base.get(key_map["menciones"], "")).split(";") if m.strip()] or [base.get(key_map["menciones"])]:
            new = deepcopy(base); new[key_map["menciones"]] = m; split_rows.append(new)
    for idx, row in enumerate(split_rows): row.update({"original_index": idx, "is_duplicate": False})
    processed_rows = detectar_duplicados_avanzado(split_rows, key_map)
    for row in processed_rows:
        if row["is_duplicate"]: row.update({"tonoai": "Duplicada", "tema": "Duplicada", "justificaciontono": "Noticia duplicada."})
    return processed_rows, key_map

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

# ======================================
# Generación de Excel
# ======================================
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
# Proceso Principal y UI
# ======================================
def run_full_process(dossier_file, region_file, internet_file, sov_file):
    # Iniciar temporizador
    tiempo_inicio = time.time()
    
    # Crear contenedor para la barra de progreso general
    progress_container = st.empty()
    status_text = st.empty()
    
    # Paso 1: Limpieza y preparación (0-30%)
    progress_container.progress(0.0)
    status_text.markdown("### 📋 **Paso 1/3:** Limpieza y preparación de datos...")
    
    all_processed_rows, key_map = run_base_logic(load_workbook(dossier_file, data_only=True).active)
    progress_container.progress(0.15)
    
    all_processed_rows = process_mappings_and_links(all_processed_rows, key_map, region_file, internet_file)
    progress_container.progress(0.30)
    status_text.markdown("### ✅ **Paso 1/3:** Base de datos preparada")
    time.sleep(0.5)
    
    for row in all_processed_rows: row["__is_target_brand"] = (row.get(key_map.get("menciones")) in TARGET_BRANDS)
    index_to_row_map = {row['original_index']: row for row in all_processed_rows}
    target_rows_all = [row for row in all_processed_rows if row.get("__is_target_brand") and not row.get("is_duplicate")]

    if not target_rows_all:
        st.warning("No se encontraron noticias únicas para las marcas objetivo para analizar.")
    else:
        # Paso 2: Análisis de IA (30-80%)
        status_text.markdown("### 🧠 **Paso 2/3:** Analizando Tono y Tema con modelos locales...")
        progress_container.progress(0.30)
        
        # Agrupar noticias similares
        status_text.markdown("### 🔗 **Paso 2/3:** Identificando noticias similares...")
        grupos_similares = agrupar_noticias_similares(target_rows_all, key_map)
        progress_container.progress(0.33)
        
        # Obtener solo los representantes de cada grupo
        representantes_indices = list(grupos_similares.keys())
        noticias_representantes = [target_rows_all[i] for i in representantes_indices]
        
        status_text.markdown(f"### 📊 **Paso 2/3:** {len(noticias_representantes)} grupos únicos identificados de {len(target_rows_all)} noticias")
        time.sleep(0.3)
        
        # Cargar modelos
        tokenizer_sent, model_sent = cargar_modelo_sentimiento()
        progress_container.progress(0.38)
        
        # Análisis de tono SOLO para representantes
        status_text.markdown(f"### 📊 **Paso 2/3:** Analizando tono para {len(noticias_representantes)} grupos únicos...")
        cache_tonos = {}  # {idx_representante: tono}
        
        for i, idx_rep in enumerate(representantes_indices):
            row = target_rows_all[idx_rep]
            texto = f"{corregir_texto(row.get(key_map.get('titulo'), ''))}. {corregir_texto(row.get(key_map.get('resumen'), ''))}"
            tono = analizar_tono_local(texto, tokenizer_sent, model_sent)
            cache_tonos[idx_rep] = tono
            
            # Actualizar progreso (38% - 52%)
            if (i + 1) % 3 == 0 or i == len(noticias_representantes) - 1:
                progreso = 0.38 + (0.14 * (i + 1) / len(noticias_representantes))
                progress_container.progress(progreso)
                status_text.markdown(f"### 📊 **Paso 2/3:** Tono analizado: {i+1}/{len(noticias_representantes)} grupos")
        
        # Aplicar tonos a todos los miembros del grupo
        for idx_rep, grupo in grupos_similares.items():
            tono = cache_tonos[idx_rep]
            for idx in grupo:
                index_to_row_map[target_rows_all[idx]['original_index']][key_map.get("tonoai")] = tono
        
        progress_container.progress(0.55)
        time.sleep(0.3)
        
        # Análisis de temas SOLO para representantes
        status_text.markdown("### 🔍 **Paso 2/3:** Asignando temas a grupos únicos...")
        progress_container.progress(0.55)
        
        modelo_emb = cargar_modelo_embeddings()
        progress_container.progress(0.60)
        
        embeddings_temas = generar_embeddings_temas()
        progress_container.progress(0.65)
        
        cache_temas = {}  # {idx_representante: tema}
        for i, idx_rep in enumerate(representantes_indices):
            noticia = target_rows_all[idx_rep]
            texto = f"{corregir_texto(noticia.get(key_map.get('titulo'), ''))}. {corregir_texto(noticia.get(key_map.get('resumen'), ''))}"
            tema = asignar_tema_por_similitud(texto, modelo_emb, embeddings_temas)
            cache_temas[idx_rep] = tema
            
            # Actualizar progreso (65% - 80%)
            if (i + 1) % 3 == 0 or i == len(noticias_representantes) - 1:
                progreso = 0.65 + (0.15 * (i + 1) / len(noticias_representantes))
                progress_container.progress(progreso)
                status_text.markdown(f"### 🔍 **Paso 2/3:** Temas asignados: {i+1}/{len(noticias_representantes)} grupos")
        
        # Aplicar temas a todos los miembros del grupo
        for idx_rep, grupo in grupos_similares.items():
            tema = cache_temas[idx_rep]
            for idx in grupo:
                index_to_row_map[target_rows_all[idx]['original_index']][key_map.get("tema")] = tema
        
        progress_container.progress(0.80)
        status_text.markdown("### ✅ **Paso 2/3:** Análisis de IA completado")
        time.sleep(0.5)

    # Paso 3: Generación de informe (80-100%)
    status_text.markdown("### 📊 **Paso 3/3:** Aplicando mapeo SOV...")
    progress_container.progress(0.80)
    
    final_processed_rows = list(index_to_row_map.values())
    final_processed_rows = process_sov_mapping_final(final_processed_rows, key_map, sov_file)
    progress_container.progress(0.90)
    
    status_text.markdown("### 📊 **Paso 3/3:** Generando archivo Excel...")
    st.session_state["output_data"] = generate_two_sheet_excel(final_processed_rows, key_map)
    st.session_state["output_filename"] = f"Informe_Analisis_UNAL_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    st.session_state["processing_complete"] = True
    
    # Calcular tiempo total
    tiempo_total = time.time() - tiempo_inicio
    st.session_state["tiempo_procesamiento"] = tiempo_total
    
    progress_container.progress(1.0)
    status_text.markdown("### ✅ **Paso 3/3:** Informe generado exitosamente")
    time.sleep(0.5)

def main():
    load_custom_css()
    if not check_password(): return

    st.markdown('<div class="main-header">🎓 Sistema de Análisis de Noticias para la Universidad Nacional</div>', unsafe_allow_html=True)
    st.markdown(f"Esta herramienta utiliza **modelos de IA locales** para analizar Tono y clasificar noticias en **{NUM_TEMAS} categorías temáticas predefinidas** para las marcas objetivo.")
    
    with st.expander("📋 Ver los 30 temas predefinidos", expanded=False):
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
        
        # Mostrar tiempo de procesamiento
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

    st.markdown("<hr><div style='text-align:center;color:#666;font-size:0.9rem;'><p>Sistema de Análisis de Noticias v10.0 (Sin API) | Universidad Nacional de Colombia</p></div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()

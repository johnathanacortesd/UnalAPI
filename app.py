# ==============================================================================
# SISTEMA DE ANÁLISIS DE MEDIOS - UNAL
# Limpieza: lógica App2 (embeddings, dedup avanzado, normalización)
# Tono/Tema: lógica App1 (GPT-4.1-nano, agrupación, consolidación)
#            → SOLO para "Universidad Nacional de Colombia - General"
# Otras marcas: solo limpieza, sin tono/tema
# Salida: Hoja 1 "UNAL con IA" | Hoja 2 "Todas las Marcas"
# ==============================================================================

import streamlit as st
import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, Alignment
from collections import defaultdict, Counter
from difflib import SequenceMatcher
from copy import deepcopy
import datetime
import io
import re
import json
import time
import html
import asyncio
import hashlib
import gc
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from functools import lru_cache
import warnings

import numpy as np
from unidecode import unidecode
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import AgglomerativeClustering
from concurrent.futures import ThreadPoolExecutor, as_completed
from thefuzz import fuzz
import openai

warnings.filterwarnings("ignore")

# ==============================================================================
# CONSTANTES
# ==============================================================================
UNAL_BRAND = "Universidad Nacional de Colombia - General"

OPENAI_MODEL = "gpt-4.1-nano-2025-04-14"
OPENAI_EMBED = "text-embedding-3-small"

SIMILARITY_THRESHOLD_TITULOS = 0.93
SIMILARITY_THRESHOLD_TONO    = 0.82
MAX_WORKERS_TONO             = 5

PRICE_INPUT_1M     = 0.10
PRICE_OUTPUT_1M    = 0.40
PRICE_EMBEDDING_1M = 0.02

# ==============================================================================
# PAGE CONFIG
# ==============================================================================
st.set_page_config(
    page_title="Análisis UNAL · IA",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ==============================================================================
# CSS
# ==============================================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Roboto+Mono:wght@400;500&display=swap');
:root {
    --bg:#f0f4f8; --s1:#ffffff; --s2:#f8fafc; --border:#e2e8f0;
    --text:#1a202c; --text2:#4a5568; --text3:#718096;
    --unal:#003087; --unal2:#0050b3; --unal-light:#e8f0fe; --unal-bdr:#b3cdf7;
    --green:#059669; --green-bg:#ecfdf5; --green-bdr:#a7f3d0;
    --amber:#d97706; --red:#dc2626;
    --r:8px; --r2:12px; --r3:16px;
    --shadow:0 1px 3px rgba(0,0,0,0.08),0 4px 12px rgba(0,0,0,0.04);
}
html,body,[data-testid="stApp"]{
    background:var(--bg)!important; color:var(--text)!important;
    font-family:'Inter',-apple-system,sans-serif; font-size:14px;
}
#MainMenu,footer,header{visibility:hidden} .stDeployButton{display:none}
.block-container{padding-top:1.2rem!important}

/* HEADER */
.app-header{
    background:linear-gradient(135deg,#003087 0%,#0050b3 60%,#1a73e8 100%);
    border-radius:var(--r3); padding:1.4rem 2rem; margin-bottom:1.2rem;
    display:flex; align-items:center; gap:1.2rem; position:relative; overflow:hidden;
    box-shadow:0 4px 20px rgba(0,48,135,0.3);
}
.app-header::after{
    content:''; position:absolute; right:-40px; top:-40px;
    width:180px; height:180px; border-radius:50%;
    background:rgba(255,255,255,0.06);
}
.app-header-icon{
    width:52px; height:52px; background:rgba(255,255,255,0.15);
    border-radius:14px; display:flex; align-items:center;
    justify-content:center; font-size:1.6rem; flex-shrink:0;
    backdrop-filter:blur(8px); border:1px solid rgba(255,255,255,0.2);
}
.app-header-title{
    font-size:1.35rem; font-weight:700; color:#fff; letter-spacing:-0.01em;
}
.app-header-sub{font-size:0.8rem; color:rgba(255,255,255,0.72); margin-top:0.2rem;}
.app-header-badge{
    margin-left:auto; background:rgba(255,255,255,0.18);
    border:1px solid rgba(255,255,255,0.3); color:#fff;
    font-family:'Roboto Mono',monospace; font-size:0.62rem;
    font-weight:500; padding:0.3rem 0.9rem; border-radius:100px;
    letter-spacing:0.06em; text-transform:uppercase; white-space:nowrap;
    backdrop-filter:blur(8px);
}

/* CARDS */
.metric-grid{display:grid; grid-template-columns:repeat(4,1fr); gap:0.8rem; margin:1rem 0;}
.metric-card{
    background:var(--s1); border:1px solid var(--border);
    border-radius:var(--r2); padding:1rem 1.1rem;
    box-shadow:var(--shadow); position:relative; overflow:hidden;
}
.metric-card::before{
    content:''; position:absolute; top:0; left:0; right:0; height:3px;
    border-radius:var(--r2) var(--r2) 0 0;
}
.mc-blue::before{background:linear-gradient(90deg,#003087,#1a73e8);}
.mc-green::before{background:linear-gradient(90deg,#059669,#34d399);}
.mc-amber::before{background:linear-gradient(90deg,#d97706,#fbbf24);}
.mc-purple::before{background:linear-gradient(90deg,#7c3aed,#a78bfa);}
.metric-val{font-size:1.7rem; font-weight:700; line-height:1; letter-spacing:-0.02em;}
.metric-lbl{font-size:0.68rem; color:var(--text3); text-transform:uppercase;
    letter-spacing:0.07em; margin-top:0.3rem; font-family:'Roboto Mono',monospace;}

/* COST BOX */
.cost-box{
    background:var(--unal-light); border:1px solid var(--unal-bdr);
    border-radius:var(--r2); padding:0.8rem 1.2rem; margin:0.8rem 0;
    font-family:'Roboto Mono',monospace; font-size:0.78rem; color:var(--unal2);
    display:flex; flex-wrap:wrap; gap:0.5rem 2rem;
}
.cost-box b{color:var(--unal); font-weight:600;}

/* SUCCESS */
.success-banner{
    background:var(--green-bg); border:1px solid var(--green-bdr);
    border-left:4px solid var(--green); border-radius:var(--r2);
    padding:0.9rem 1.3rem; margin:0.8rem 0;
    display:flex; align-items:center; gap:0.9rem;
}
.success-icon{
    width:36px; height:36px;
    background:linear-gradient(135deg,#059669,#047857);
    border-radius:50%; display:flex; align-items:center;
    justify-content:center; color:#fff; font-size:1.1rem; flex-shrink:0;
}
.success-title{font-weight:700; color:#047857; font-size:1rem;}
.success-sub{font-size:0.78rem; color:var(--text2);}

/* INFO BOX */
.info-box{
    background:var(--unal-light); border-left:3px solid var(--unal);
    border-radius:0 var(--r) var(--r) 0; padding:0.7rem 1rem;
    font-size:0.85rem; color:var(--text2); margin:0.5rem 0;
}

/* PHASE LABEL */
.phase-lbl{
    font-size:0.7rem; font-weight:700; color:var(--unal2);
    letter-spacing:0.1em; text-transform:uppercase;
    display:flex; align-items:center; gap:0.5rem;
    padding:0.4rem 0; border-bottom:2px solid var(--unal-light);
    margin:0.8rem 0 0.5rem;
}

/* AUTH */
.auth-wrap{max-width:360px; margin:8vh auto 0; text-align:center;}
.auth-icon{
    width:64px; height:64px; margin:0 auto 1rem;
    background:linear-gradient(135deg,#003087,#1a73e8);
    border-radius:18px; display:flex; align-items:center;
    justify-content:center; font-size:1.8rem; color:#fff;
    box-shadow:0 6px 20px rgba(0,48,135,0.35);
}
.auth-title{font-size:1.4rem; font-weight:700; color:var(--text);}
.auth-sub{font-size:0.85rem; color:var(--text3); margin:0.4rem 0 1.8rem;}

/* BUTTONS */
.stButton>button,[data-testid="stDownloadButton"]>button{
    border-radius:100px!important; font-weight:500!important;
    transition:all 0.2s!important;
}
.stButton>button[kind="primary"],[data-testid="stDownloadButton"]>button{
    background:var(--unal)!important; color:#fff!important;
    border:none!important; font-size:0.92rem!important;
    padding:0.6rem 1.6rem!important;
    box-shadow:0 2px 8px rgba(0,48,135,0.3)!important;
}
.stButton>button[kind="primary"]:hover{
    background:var(--unal2)!important;
    box-shadow:0 4px 16px rgba(0,48,135,0.4)!important;
    transform:translateY(-1px)!important;
}

[data-testid="stProgressBar"]>div>div{
    background:linear-gradient(90deg,#003087,#1a73e8)!important;
    border-radius:100px!important; height:5px!important;
}
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# SESSION STATE
# ==============================================================================
for _k, _v in {
    "password_correct": False,
    "processing_complete": False,
    "output_data": None,
    "output_filename": "",
    "stats": {},
    "tokens_input": 0,
    "tokens_output": 0,
    "tokens_embedding": 0,
}.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ==============================================================================
# AUTH
# ==============================================================================
def check_password() -> bool:
    if st.session_state["password_correct"]:
        return True
    st.markdown("""
    <div class="auth-wrap">
        <div class="auth-icon">🎓</div>
        <div class="auth-title">Sistema de Análisis UNAL</div>
        <div class="auth-sub">Ingresa la contraseña para continuar</div>
    </div>""", unsafe_allow_html=True)
    _, col, _ = st.columns([1, 2, 1])
    with col:
        with st.form("pw_form"):
            pw = st.text_input("Contraseña", type="password", placeholder="••••••••")
            if st.form_submit_button("Ingresar", use_container_width=True, type="primary"):
                correct = st.secrets.get("APP_PASSWORD", "")
                if pw == correct:
                    st.session_state["password_correct"] = True
                    st.rerun()
                else:
                    st.error("Contraseña incorrecta.")
    return False

# ==============================================================================
# UTILIDADES DE TEXTO
# ==============================================================================
@lru_cache(maxsize=20000)
def norm_key(text: Any) -> str:
    if text is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

def normalize_title_for_comparison(title: Any) -> str:
    if not isinstance(title, str):
        return ""
    tmp = re.split(r"\s*[:|-]\s*", title, 1)
    return re.sub(r"\W+", " ", tmp[0]).lower().strip()

def clean_title_for_output(title: Any) -> str:
    return re.sub(r"\s*\|\s*[\w\s]+$", "", str(title or "")).strip()

def convert_html_entities(text: str) -> str:
    if not isinstance(text, str):
        return text
    text = html.unescape(text)
    text = re.sub(r'&#x([0-9A-Fa-f]+);', lambda m: chr(int(m.group(1), 16)), text)
    text = re.sub(r'&#(\d+);',           lambda m: chr(int(m.group(1))),       text)
    for bad, good in {'\u201c': '"', '\u201d': '"', '\u2018': "'", '\u2019': "'"}.items():
        text = text.replace(bad, good)
    return text

def clean_text(text: Any) -> str:
    if not isinstance(text, str):
        return str(text) if text is not None else ""
    return convert_html_entities(text).strip()

def clean_cuerpo(text: Any) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    text = convert_html_entities(text)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    return text.strip()

def corregir_texto_resumen(text: Any) -> str:
    if not isinstance(text, str):
        return ""
    text = re.sub(r'(<br\s*/?>|\[\.\.\.\])+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    m = re.search(r"[A-ZÁÉÍÓÚÑ]", text)
    if m:
        text = text[m.start():]
    return text

def texto_para_analisis(titulo: Any, resumen: Any, max_len: int = 2000) -> str:
    t = str(titulo or "").strip()
    r = str(resumen or "").strip()
    return f"TÍTULO: {t}. RESUMEN: {r}"[:max_len]

def _normalizar_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip().lower()
    url = re.sub(r'^https?://', '', url)
    url = re.sub(r'^www\.', '', url)
    return url.rstrip('/')

def normalizar_tipo_medio(tipo_raw: Any) -> str:
    if not isinstance(tipo_raw, str):
        return str(tipo_raw or "Otro")
    t = unidecode(tipo_raw.strip().lower())
    return {
        'online': 'Internet', 'internet': 'Internet', 'digital': 'Internet', 'web': 'Internet',
        'diario': 'Prensa', 'prensa': 'Prensa',
        'am': 'Radio', 'fm': 'Radio', 'radio': 'Radio',
        'aire': 'Televisión', 'cable': 'Televisión', 'tv': 'Televisión',
        'television': 'Televisión', 'televisión': 'Televisión',
        'senal abierta': 'Televisión', 'señal abierta': 'Televisión',
        'revista': 'Revista', 'revistas': 'Revista',
    }.get(t, tipo_raw.strip().title() or "Otro")

def parse_numeric(val: Any):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val) if isinstance(val, float) and val.is_integer() else val
    s = str(val).strip()
    if not s:
        return None
    try:
        s2 = s.replace(',', '.')
        f = float(s2)
        return int(f) if f.is_integer() else f
    except ValueError:
        return None

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================
def load_local_config() -> Optional[Path]:
    for name in ["Configuracion.xlsx", "configuracion.xlsx", "Config.xlsx", "config.xlsx"]:
        p = Path(name)
        if p.exists():
            return p
    for f in Path(__file__).parent.iterdir():
        if f.suffix.lower() == '.xlsx' and 'config' in f.stem.lower():
            return f
    return None

def load_config(source) -> Tuple[Dict, Dict]:
    sheets = pd.read_excel(source, sheet_name=None, engine='openpyxl')
    region_map = pd.Series(
        sheets['Regiones'].iloc[:, 1].values,
        index=sheets['Regiones'].iloc[:, 0].astype(str).str.lower().str.strip()
    ).to_dict()
    internet_map = pd.Series(
        sheets['Internet'].iloc[:, 1].values,
        index=sheets['Internet'].iloc[:, 0].astype(str).str.lower().str.strip()
    ).to_dict()
    return region_map, internet_map

# ==============================================================================
# LECTURA Y NORMALIZACIÓN DEL DOSSIER (lógica App2 adaptada)
# ==============================================================================
def extract_link(cell) -> Dict:
    if hasattr(cell, "hyperlink") and cell.hyperlink and cell.hyperlink.target:
        return {"value": cell.value or "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        m = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if m:
            return {"value": "Link", "url": m.group(1)}
    return {"value": cell.value, "url": None}

def read_and_normalize_dossier(sheet, region_map: Dict, internet_map: Dict) -> List[Dict]:
    """
    Lee el sheet activo del workbook openpyxl, normaliza cada fila y
    devuelve una lista de dicts (una fila por mención si hay ;).
    """
    headers = [cell.value for cell in sheet[1] if cell.value is not None]
    raw_rows = []
    for row in sheet.iter_rows(min_row=2):
        if all(c.value is None for c in row):
            continue
        rd: Dict = {}
        for i, h in enumerate(headers):
            if i >= len(row):
                break
            cell = row[i]
            # Detectar hyperlink
            url = None
            if hasattr(cell, 'hyperlink') and cell.hyperlink and cell.hyperlink.target:
                url = cell.hyperlink.target
            elif isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
                m = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
                if m:
                    url = m.group(1)
            if url:
                rd[h] = {"value": cell.value or "Link", "url": url}
            else:
                rd[h] = cell.value
        raw_rows.append(rd)

    # Normalizar tipo de medio
    tipo_map = {
        'online': 'Internet', 'internet': 'Internet', 'digital': 'Internet', 'web': 'Internet',
        'diario': 'Prensa', 'prensa': 'Prensa',
        'am': 'Radio', 'fm': 'Radio', 'radio': 'Radio',
        'aire': 'Televisión', 'cable': 'Televisión', 'tv': 'Televisión',
        'television': 'Televisión', 'televisión': 'Televisión',
        'senal abierta': 'Televisión', 'señal abierta': 'Televisión',
        'revista': 'Revista', 'revistas': 'Revista',
    }

    expanded: List[Dict] = []
    for orig_idx, rd in enumerate(raw_rows):
        # Tipo de medio
        tipo_raw = rd.get('Tipo de Medio', rd.get('tipo de medio', ''))
        tipo_str = str(tipo_raw or '').strip()
        tipo_norm = tipo_map.get(unidecode(tipo_str.lower()), tipo_str.title() or 'Otro')

        is_av      = tipo_norm in ('Radio', 'Televisión')
        is_internet = tipo_norm == 'Internet'
        is_grafica  = tipo_norm in ('Prensa', 'Internet', 'Revista')

        # Región (antes de cambiar Medio)
        medio_raw = rd.get('Medio', rd.get('medio', ''))
        medio_str = str(medio_raw if not isinstance(medio_raw, dict) else medio_raw.get('value', '')).lower().strip()
        region = region_map.get(medio_str, 'N/A')

        # Medio → renombrar con internet_map si aplica
        medio_final = medio_raw
        if is_internet:
            medio_mapeado = internet_map.get(medio_str)
            if medio_mapeado:
                medio_final = medio_mapeado

        # Título
        titulo = clean_text(rd.get('Título', rd.get('Titulo', rd.get('título', ''))))

        # Resumen/Cuerpo
        cuerpo_raw = rd.get('Resumen - Aclaracion', rd.get('CuerpoEs', rd.get('resumen', '')))
        resumen = clean_cuerpo(str(cuerpo_raw or ''))

        # CPE
        cpe_av      = rd.get('CPE')
        cpe_grafica = rd.get('Valor de Nota')
        cpe = cpe_av if is_av else (cpe_grafica if is_grafica else None)

        # Dimensión / Duración
        dim_raw = rd.get('Dimensión', rd.get('Dimensioncm2', rd.get('dimension', '')))
        dur_raw = rd.get('Duración - Nro. Caracteres', rd.get('duracion', ''))
        if is_av:
            dimension = dur_raw
            duracion  = 0
        else:
            dimension = dim_raw
            duracion  = dur_raw

        # Links
        def _get_url(val):
            if isinstance(val, dict):
                return val.get('url')
            s = str(val or '')
            return s if s.startswith('http') else None

        url_nota_av_raw = rd.get('URL Nota AV', rd.get('Link Nota AV', ''))
        url_streaming_raw = rd.get('URL (Streaming - Imagen)', rd.get('Link (Streaming - Imagen)', ''))
        url_nota_raw = rd.get('URL Nota', rd.get('Link Nota', ''))

        if is_av:
            u = _get_url(url_nota_av_raw)
            u = u.replace('.com.ar', '.com.co') if u else u
            link_nota = {"value": "Link", "url": u}
            link_streaming = None
        elif is_internet:
            u_stream = _get_url(url_streaming_raw)
            u_nota   = _get_url(url_nota_raw)
            link_nota      = {"value": "Link", "url": u_stream}
            link_streaming = {"value": "Link", "url": u_nota}
        else:
            # Prensa / Revista
            u_s = _get_url(url_streaming_raw)
            u_n = _get_url(url_nota_raw)
            link_nota      = {"value": "Link", "url": u_s or u_n}
            link_streaming = None

        # Menciones
        menc_av      = clean_text(str(rd.get('Menciones - Empresa', '') or ''))
        menc_grafica = clean_text(str(rd.get('Empresa rel.', '') or ''))
        menciones_str = menc_av if is_av else (menc_grafica if is_grafica else menc_av)

        base: Dict = {
            'ID Noticia':                   rd.get('ID Noticia', rd.get('NoticiaId', '')),
            'Fecha':                         rd.get('Fecha', ''),
            'Hora':                          rd.get('Hora', ''),
            'Medio':                         medio_final,
            'Tipo de Medio':                 tipo_norm,
            'Sección - Programa':            clean_text(str(rd.get('Sección - Programa', rd.get('Seccion - Programa', '')) or '')),
            'Región':                         region,
            'Título':                         titulo,
            'Autor - Conductor':             clean_text(str(rd.get('Autor - Conductor', '') or '')),
            'Nro. Pagina':                   rd.get('Nro. Pagina', ''),
            'Dimensión':                      dimension,
            'Duración - Nro. Caracteres':    duracion,
            'CPE':                           cpe,
            'Tier':                          rd.get('Tier', ''),
            'Audiencia':                     rd.get('Audiencia', ''),
            'Tono':                          clean_text(str(rd.get('Tono', '') or '')),
            'Tono AI':                        '',
            'Tema':                          '',
            'Link Nota':                     link_nota,
            'Resumen - Aclaracion':          resumen,
            'Link (Streaming - Imagen)':     link_streaming,
            'ID duplicada':                  '',
            '__orig_idx':                    orig_idx,
            'is_duplicate':                  False,
        }

        # Expandir por ";" en menciones
        menciones_list = [m.strip() for m in menciones_str.split(';') if m.strip()]
        if not menciones_list:
            menciones_list = ['']
        for menc in menciones_list:
            row_copy = dict(base)
            row_copy['Menciones - Empresa'] = menc
            expanded.append(row_copy)

    return expanded

# ==============================================================================
# DETECCIÓN DE DUPLICADOS (lógica App2 adaptada)
# ==============================================================================
def detectar_duplicados(rows: List[Dict]) -> List[Dict]:
    processed = deepcopy(rows)
    seen_url: Dict     = {}
    seen_bcast: Dict   = {}
    seen_stream: Dict  = {}
    internet_groups: Dict[tuple, List[int]] = defaultdict(list)

    for i, row in enumerate(processed):
        if row.get('is_duplicate'):
            continue

        tipo    = normalizar_tipo_medio(str(row.get('Tipo de Medio', '')))
        mencion = norm_key(row.get('Menciones - Empresa', ''))
        medio   = norm_key(str(row.get('Medio', '') if not isinstance(row.get('Medio'), dict) else row['Medio'].get('value', '')))

        # Dedup por link streaming
        ls = row.get('Link (Streaming - Imagen)')
        ls_url = ls.get('url') if isinstance(ls, dict) else None
        if ls_url and mencion:
            sk = (_normalizar_url(ls_url), mencion)
            if sk in seen_stream:
                row['is_duplicate'] = True
                row['ID duplicada'] = processed[seen_stream[sk]].get('ID Noticia', '')
                continue
            seen_stream[sk] = i

        if tipo == 'Internet':
            ln = row.get('Link Nota')
            url = ln.get('url') if isinstance(ln, dict) else None
            if url and mencion:
                k = (_normalizar_url(url), mencion)
                if k in seen_url:
                    row['is_duplicate'] = True
                    row['ID duplicada'] = processed[seen_url[k]].get('ID Noticia', '')
                    continue
                seen_url[k] = i
            if medio and mencion:
                internet_groups[(medio, mencion)].append(i)

        elif tipo in ('Radio', 'Televisión'):
            hora = str(row.get('Hora', '')).strip()
            if mencion and medio and hora:
                k = (mencion, medio, hora)
                if k in seen_bcast:
                    row['is_duplicate'] = True
                    row['ID duplicada'] = processed[seen_bcast[k]].get('ID Noticia', '')
                else:
                    seen_bcast[k] = i

    # Dedup por título similar en internet agrupado por medio+mención
    for idxs in internet_groups.values():
        if len(idxs) < 2:
            continue
        for a in range(len(idxs)):
            for b in range(a + 1, len(idxs)):
                ia, ib = idxs[a], idxs[b]
                if processed[ia].get('is_duplicate') or processed[ib].get('is_duplicate'):
                    continue
                ta = normalize_title_for_comparison(processed[ia].get('Título', ''))
                tb = normalize_title_for_comparison(processed[ib].get('Título', ''))
                if ta and tb and SequenceMatcher(None, ta, tb).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                    older = ia if len(ta) >= len(tb) else ib
                    newer = ib if older == ia else ia
                    processed[newer]['is_duplicate'] = True
                    processed[newer]['ID duplicada'] = processed[older].get('ID Noticia', '')

    return processed

# ==============================================================================
# CACHÉ DE EMBEDDINGS
# ==============================================================================
class EmbeddingCache:
    def __init__(self):
        self._cache: Dict[str, List[float]] = {}

    def _key(self, text: str) -> str:
        return hashlib.md5(text[:2000].encode('utf-8', errors='ignore')).hexdigest()

    def get(self, text: str):
        return self._cache.get(self._key(text))

    def put(self, text: str, emb: List[float]):
        self._cache[self._key(text)] = emb

    def clear(self):
        self._cache.clear()

if '_emb_cache' not in st.session_state:
    st.session_state['_emb_cache'] = EmbeddingCache()

def _emb_cache() -> EmbeddingCache:
    return st.session_state['_emb_cache']

def get_embeddings_batch(textos: List[str], batch_size: int = 100) -> List[Optional[List[float]]]:
    if not textos:
        return []
    cache = _emb_cache()
    results: List[Optional[List[float]]] = [None] * len(textos)
    missing: List[int] = []
    for i, t in enumerate(textos):
        cached = cache.get(t)
        if cached is not None:
            results[i] = cached
        else:
            missing.append(i)
    if not missing:
        return results
    for start in range(0, len(missing), batch_size):
        batch_idx = missing[start:start + batch_size]
        batch_txt = [textos[i][:2000] for i in batch_idx]
        try:
            resp = openai.Embedding.create(input=batch_txt, model=OPENAI_EMBED)
            st.session_state['tokens_embedding'] += resp.get('usage', {}).get('total_tokens', 0)
            for j, d in enumerate(resp['data']):
                oi = batch_idx[j]
                results[oi] = d['embedding']
                cache.put(textos[oi], d['embedding'])
        except Exception as e:
            st.warning(f"Error en embeddings (lote {start}): {e}")
    return results

# ==============================================================================
# AGRUPACIÓN DE NOTICIAS SIMILARES (para análisis UNAL)
# ==============================================================================
def agrupar_noticias_unal(rows: List[Dict]) -> List[List[int]]:
    """
    Agrupa índices de filas UNAL no duplicadas por similitud de título/resumen.
    Usa Union-Find sobre las primeras palabras del título + fallback por embeddings.
    """
    n = len(rows)
    parent = list(range(n))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i, j):
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[rj] = ri

    # Paso 1: Primeras palabras del título
    titulo_map: Dict[str, int] = {}
    resumen_map: Dict[str, int] = {}
    for i, row in enumerate(rows):
        titulo = str(row.get('Título', '') or '')
        resumen = str(row.get('Resumen - Aclaracion', '') or '')
        tk = norm_key(' '.join(titulo.split()[:4]))
        rk = norm_key(' '.join(resumen.split()[:6]))
        if tk:
            if tk in titulo_map:
                union(i, titulo_map[tk])
            else:
                titulo_map[tk] = i
        if rk:
            if rk in resumen_map:
                union(i, resumen_map[rk])
            else:
                resumen_map[rk] = i

    # Paso 2: Similitud por embeddings (clustering semántico)
    textos = [texto_para_analisis(row.get('Título', ''), row.get('Resumen - Aclaracion', '')) for row in rows]
    embs = get_embeddings_batch(textos)
    validos = [(i, embs[i]) for i in range(n) if embs[i] is not None]
    if len(validos) >= 2:
        idxs_v, M = zip(*validos)
        try:
            labels = AgglomerativeClustering(
                n_clusters=None,
                distance_threshold=1 - SIMILARITY_THRESHOLD_TONO,
                metric='cosine',
                linkage='average'
            ).fit(np.array(M)).labels_
            grupos_cl: Dict[int, List[int]] = defaultdict(list)
            for k, lbl in enumerate(labels):
                grupos_cl[lbl].append(idxs_v[k])
            for miembros in grupos_cl.values():
                for j in miembros[1:]:
                    union(miembros[0], j)
        except Exception:
            pass

    grupos_finales: Dict[int, List[int]] = defaultdict(list)
    for i in range(n):
        grupos_finales[find(i)].append(i)
    return list(grupos_finales.values())

# ==============================================================================
# ANÁLISIS TONO / TEMA CON GPT (lógica App1 mejorada)
# ==============================================================================
SYSTEM_PROMPT_UNAL = """Eres un analista de medios especializado en evaluar el impacto de noticias sobre la Universidad Nacional de Colombia (UNAL). Aplica estas reglas sin excepción.

**REGLA DE ORO:**
Si la UNAL NO es el actor principal, o su mención es contextual/referencial/fuente de opinión → SIEMPRE NEUTRO, independientemente del contenido de la noticia.

**TONO (solo si UNAL es actor principal):**
- NEGATIVO: fallo directo de UNAL o evento perjudicial bajo su responsabilidad (críticas a la gestión, disturbios en campus, escándalos internos).
- POSITIVO: logro o acción destacada de UNAL (premios, reconocimientos, avances científicos, conciertos/eventos organizados por UNAL).
- NEUTRO: menciones informativas que no constituyan logro ni fallo; la UNAL no es actor principal.

**TEMA (4 a 6 palabras exactas):**
- Describe el hecho principal del grupo de noticias.
- NO incluyas "Universidad Nacional", "UNAL" ni variantes.
- Longitud estricta: 4-6 palabras."""

def _llamar_api_tono_tema(texto: str, grupo_idx: int) -> Dict[str, str]:
    """Llama a GPT para obtener tono y tema de un grupo."""
    tools = [{
        "type": "function",
        "function": {
            "name": "clasificar_noticia_unal",
            "description": "Clasifica el tono y tema de un grupo de noticias sobre la UNAL.",
            "parameters": {
                "type": "object",
                "properties": {
                    "tono": {
                        "type": "string",
                        "enum": ["Positivo", "Negativo", "Neutro"],
                        "description": "Tono reputacional de la UNAL en esta noticia."
                    },
                    "tema": {
                        "type": "string",
                        "description": "Tema de 4 a 6 palabras sin mencionar UNAL."
                    }
                },
                "required": ["tono", "tema"]
            }
        }
    }]
    try:
        resp = openai.ChatCompletion.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT_UNAL},
                {"role": "user",   "content": f"Analiza este grupo de noticias:\n\n{texto}"}
            ],
            tools=tools,
            tool_choice={"type": "function", "function": {"name": "clasificar_noticia_unal"}},
            temperature=0.0,
            max_tokens=150,
        )
        usage = resp.get('usage', {})
        st.session_state['tokens_input']  += usage.get('prompt_tokens', 0)
        st.session_state['tokens_output'] += usage.get('completion_tokens', 0)
        args = json.loads(resp.choices[0].message.tool_calls[0].function.arguments)
        return {"tono": args.get("tono", "Neutro"), "tema": args.get("tema", "")}
    except Exception as e:
        return {"tono": "Error", "tema": "Excepción API"}

def analizar_tono_tema_unal(
    rows_unal: List[Dict],
    progress_bar,
    status_text,
) -> List[Dict]:
    """
    Agrupa noticias UNAL similares, analiza tono/tema con GPT en paralelo
    y aplica los resultados a todas las filas del grupo.
    Devuelve la lista rows_unal con 'Tono AI' y 'Tema' rellenos.
    """
    n = len(rows_unal)
    if n == 0:
        return rows_unal

    status_text.markdown('<div class="info-box">🔗 Agrupando noticias similares...</div>', unsafe_allow_html=True)
    progress_bar.progress(0.05, "Agrupando noticias UNAL...")

    grupos = agrupar_noticias_unal(rows_unal)
    ng = len(grupos)

    # Preparar textos representativos por grupo
    textos_agrupados: List[Tuple[str, List[int]]] = []
    for grupo_idxs in grupos:
        repr_idx = grupo_idxs[0]
        texto = texto_para_analisis(
            rows_unal[repr_idx].get('Título', ''),
            rows_unal[repr_idx].get('Resumen - Aclaracion', '')
        )
        if texto.strip() and texto != "TÍTULO: . RESUMEN: ":
            textos_agrupados.append((texto, grupo_idxs))

    status_text.markdown(f'<div class="info-box">🤖 Analizando {len(textos_agrupados)} grupos con IA...</div>', unsafe_allow_html=True)

    resultados: Dict[int, Dict] = {}
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_TONO) as executor:
        future_to_grupo = {
            executor.submit(_llamar_api_tono_tema, texto, i): (texto, indices)
            for i, (texto, indices) in enumerate(textos_agrupados)
        }
        for future in as_completed(future_to_grupo):
            _, indices = future_to_grupo[future]
            try:
                resultado = future.result()
            except Exception:
                resultado = {"tono": "Error", "tema": "Excepción API"}
            for idx in indices:
                resultados[idx] = resultado
            completed += 1
            pct = 0.15 + 0.75 * (completed / max(len(textos_agrupados), 1))
            progress_bar.progress(pct, f"IA: {completed}/{len(textos_agrupados)} grupos")

    # Aplicar resultados
    for i, row in enumerate(rows_unal):
        r = resultados.get(i, {"tono": "Neutro", "tema": ""})
        row['Tono AI'] = r['tono']
        row['Tema']    = r['tema']

    # Consolidar temas similares (lógica App1)
    status_text.markdown('<div class="info-box">✨ Consolidando temas similares...</div>', unsafe_allow_html=True)
    progress_bar.progress(0.92, "Consolidando temas...")
    rows_unal = consolidar_temas_similares(rows_unal)

    progress_bar.progress(1.0, f"✅ {ng} grupos analizados")
    return rows_unal

def consolidar_temas_similares(rows: List[Dict], umbral: int = 85) -> List[Dict]:
    """
    Agrupa temas semánticamente similares (thefuzz token_set_ratio)
    y los normaliza al tema más corto del grupo.
    """
    temas_unicos = list(set(
        row.get('Tema', '')
        for row in rows
        if row.get('Tema') and row.get('Tema') not in ('', 'Duplicada', 'Error', 'Excepción API')
    ))
    if not temas_unicos:
        return rows

    n = len(temas_unicos)
    parent = list(range(n))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i, j):
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[rj] = ri

    for i in range(n):
        for j in range(i + 1, n):
            if fuzz.token_set_ratio(temas_unicos[i], temas_unicos[j]) >= umbral:
                union(i, j)

    grupos: Dict[int, List[int]] = defaultdict(list)
    for i in range(n):
        grupos[find(i)].append(i)

    mapa: Dict[str, str] = {}
    for miembros in grupos.values():
        grupo_temas = [temas_unicos[i] for i in miembros]
        canonico = min(grupo_temas, key=len)
        for t in grupo_temas:
            mapa[t] = canonico

    for row in rows:
        tema = row.get('Tema', '')
        if tema in mapa:
            row['Tema'] = mapa[tema]
    return rows

# ==============================================================================
# GENERACIÓN DEL EXCEL DE SALIDA
# ==============================================================================
COLUMN_ORDER = [
    "ID Noticia", "Fecha", "Hora", "Medio", "Tipo de Medio",
    "Sección - Programa", "Región", "Título", "Autor - Conductor",
    "Nro. Pagina", "Dimensión", "Duración - Nro. Caracteres",
    "CPE", "Tier", "Audiencia", "Tono", "Tono AI", "Tema",
    "Link Nota", "Resumen - Aclaracion", "Link (Streaming - Imagen)",
    "Menciones - Empresa", "ID duplicada",
]

COLUMN_ORDER_OTRAS = [
    "ID Noticia", "Fecha", "Hora", "Medio", "Tipo de Medio",
    "Sección - Programa", "Región", "Título", "Autor - Conductor",
    "Nro. Pagina", "Dimensión", "Duración - Nro. Caracteres",
    "CPE", "Tier", "Audiencia", "Tono",
    "Link Nota", "Resumen - Aclaracion", "Link (Streaming - Imagen)",
    "Menciones - Empresa", "ID duplicada",
]

NUM_COLS = {"Nro. Pagina", "Dimensión", "Duración - Nro. Caracteres", "CPE", "Tier", "Audiencia"}
FONT_HYPERLINK = Font(color="0563C1", underline="single")
FONT_BOLD      = Font(bold=True)
ALIGN_LEFT     = Alignment(horizontal='left')

def _write_sheet(ws, rows: List[Dict], columns: List[str]):
    ws.append(columns)
    for cell in ws[1]:
        cell.font = FONT_BOLD

    for row in rows:
        # Limpiar título/resumen
        row['Título'] = clean_title_for_output(row.get('Título', ''))
        row['Resumen - Aclaracion'] = corregir_texto_resumen(str(row.get('Resumen - Aclaracion', '') or ''))

        out_row = []
        links: Dict[int, str] = {}

        for ci, col in enumerate(columns, start=1):
            val = row.get(col)
            cv  = None

            if col == 'Fecha':
                if isinstance(val, (datetime.datetime, datetime.date, pd.Timestamp)):
                    cv = pd.Timestamp(val).to_pydatetime() if not isinstance(val, datetime.datetime) else val
                elif val is not None:
                    try:
                        cv = pd.to_datetime(val, dayfirst=True).to_pydatetime()
                    except Exception:
                        cv = str(val)
            elif col in NUM_COLS:
                cv = parse_numeric(val)
            elif isinstance(val, dict) and 'url' in val:
                cv = val.get('value', 'Link') or 'Link'
                if val.get('url'):
                    links[ci] = val['url']
            elif isinstance(val, str) and val.startswith('http'):
                cv = 'Link'
                links[ci] = val
            elif val is not None:
                cv = str(val)

            out_row.append(cv)

        ws.append(out_row)
        r = ws.max_row

        for ci, url in links.items():
            cell = ws.cell(row=r, column=ci)
            cell.hyperlink = url
            cell.font      = FONT_HYPERLINK
            cell.alignment = ALIGN_LEFT

        # Formato fecha
        date_ci = columns.index('Fecha') + 1
        dc = ws.cell(row=r, column=date_ci)
        if isinstance(dc.value, (datetime.datetime, datetime.date)):
            dc.number_format = 'DD/MM/YYYY'

        # Formato CPE para AV (sin notación científica)
        if 'CPE' in columns and 'Tipo de Medio' in columns:
            tipo_ci = columns.index('Tipo de Medio') + 1
            cpe_ci  = columns.index('CPE') + 1
            tipo_v  = ws.cell(row=r, column=tipo_ci).value
            cpe_c   = ws.cell(row=r, column=cpe_ci)
            if tipo_v in ('Radio', 'Televisión') and isinstance(cpe_c.value, (int, float)):
                cpe_c.number_format = '#,##0'

    # Anchos de columna
    for i, col in enumerate(columns, start=1):
        letter = ws.cell(row=1, column=i).column_letter
        if col in ('Título', 'Resumen - Aclaracion'):
            ws.column_dimensions[letter].width = 50
        elif col in ('Link Nota', 'Link (Streaming - Imagen)'):
            ws.column_dimensions[letter].width = 15
        else:
            ws.column_dimensions[letter].width = 20

def generate_excel_output(
    rows_unal:  List[Dict],
    rows_otras: List[Dict],
) -> bytes:
    wb = Workbook()

    # Hoja 1: UNAL con IA
    ws1 = wb.active
    ws1.title = "UNAL con IA"
    _write_sheet(ws1, rows_unal, COLUMN_ORDER)

    # Hoja 2: Todas las Marcas
    ws2 = wb.create_sheet("Todas las Marcas")
    _write_sheet(ws2, rows_otras, COLUMN_ORDER_OTRAS)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

# ==============================================================================
# PROCESO PRINCIPAL
# ==============================================================================
def run_analysis(dossier_file) -> None:
    """Orquesta las 4 fases y guarda resultados en session_state."""
    st.session_state.update({
        'tokens_input': 0, 'tokens_output': 0, 'tokens_embedding': 0
    })
    _emb_cache().clear()

    # Configurar API key
    api_key = st.secrets.get("OPENAI_API_KEY", "")
    if not api_key:
        st.error("❌ OPENAI_API_KEY no configurada en secrets.")
        st.stop()
    openai.api_key = api_key

    t0 = time.time()

    progress_bar  = st.progress(0, "Iniciando...")
    status_text   = st.empty()
    metrics_ph    = st.empty()

    # ── FASE 1: Configuración ──────────────────────────────────────────────
    status_text.markdown('<div class="info-box">📂 <strong>Fase 1/4</strong> · Cargando configuración...</div>', unsafe_allow_html=True)
    progress_bar.progress(0.02, "Cargando configuración...")

    config_path = load_local_config()
    if not config_path:
        st.error("❌ No se encontró Configuracion.xlsx en el repositorio.")
        st.stop()
    region_map, internet_map = load_config(config_path)

    # ── FASE 2: Lectura y limpieza ─────────────────────────────────────────
    status_text.markdown('<div class="info-box">🧹 <strong>Fase 2/4</strong> · Leyendo y limpiando el dossier...</div>', unsafe_allow_html=True)
    progress_bar.progress(0.08, "Normalizando datos...")

    wb = load_workbook(dossier_file, data_only=True)
    all_rows = read_and_normalize_dossier(wb.active, region_map, internet_map)
    all_rows = detectar_duplicados(all_rows)

    total     = len(all_rows)
    dups      = sum(1 for r in all_rows if r.get('is_duplicate'))
    unicas    = total - dups

    # Separar UNAL vs otras marcas (solo no duplicadas de UNAL van al análisis IA)
    rows_unal_all   = [r for r in all_rows if r.get('Menciones - Empresa') == UNAL_BRAND]
    rows_otras_all  = [r for r in all_rows if r.get('Menciones - Empresa') != UNAL_BRAND]

    unal_no_dup     = [r for r in rows_unal_all if not r.get('is_duplicate')]
    unal_dup        = [r for r in rows_unal_all if r.get('is_duplicate')]

    # Marcar duplicadas UNAL
    for r in unal_dup:
        r['Tono AI'] = 'Duplicada'
        r['Tema']    = 'Duplicada'

    with metrics_ph.container():
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("📰 Total filas",    f"{total:,}")
        c2.metric("🎓 Filas UNAL",     f"{len(rows_unal_all):,}")
        c3.metric("🔄 Duplicadas",     f"{dups:,}")
        c4.metric("🏢 Otras marcas",   f"{len(rows_otras_all):,}")

    progress_bar.progress(0.20, "✅ Fase 2 completada")

    # ── FASE 3: Análisis IA para UNAL ─────────────────────────────────────
    status_text.markdown('<div class="info-box">🤖 <strong>Fase 3/4</strong> · Analizando con IA (solo UNAL)...</div>', unsafe_allow_html=True)
    progress_bar.progress(0.22, "Preparando análisis UNAL...")

    sub_progress = st.progress(0, "Iniciando análisis UNAL...")
    sub_status   = st.empty()

    if unal_no_dup:
        unal_no_dup_analizadas = analizar_tono_tema_unal(
            unal_no_dup,
            sub_progress,
            sub_status,
        )
    else:
        unal_no_dup_analizadas = []
        sub_status.markdown('<div class="info-box">ℹ️ No hay noticias UNAL no duplicadas para analizar.</div>', unsafe_allow_html=True)

    progress_bar.progress(0.85, "✅ Fase 3 completada")

    # ── FASE 4: Generación del Excel ───────────────────────────────────────
    status_text.markdown('<div class="info-box">📄 <strong>Fase 4/4</strong> · Generando informe Excel...</div>', unsafe_allow_html=True)
    progress_bar.progress(0.88, "Construyendo Excel...")

    rows_unal_final  = unal_no_dup_analizadas + unal_dup
    rows_otras_final = rows_otras_all

    output_bytes = generate_excel_output(rows_unal_final, rows_otras_final)
    elapsed = time.time() - t0

    ci = (st.session_state['tokens_input']     / 1e6) * PRICE_INPUT_1M
    co = (st.session_state['tokens_output']    / 1e6) * PRICE_OUTPUT_1M
    ce = (st.session_state['tokens_embedding'] / 1e6) * PRICE_EMBEDDING_1M
    costo_total = ci + co + ce

    progress_bar.progress(1.0, "✅ ¡Completado!")
    status_text.markdown(
        '<div class="success-banner">'
        '<div class="success-icon">✓</div>'
        '<div><div class="success-title">Análisis completado</div>'
        '<div class="success-sub">El informe está listo para descargar.</div></div>'
        '</div>',
        unsafe_allow_html=True
    )

    # Guardar en session_state
    st.session_state['processing_complete'] = True
    st.session_state['output_data']     = output_bytes
    st.session_state['output_filename'] = (
        f"Informe_UNAL_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    )
    st.session_state['stats'] = {
        'total':        total,
        'unal':         len(rows_unal_all),
        'unal_analiz':  len(unal_no_dup_analizadas),
        'otras':        len(rows_otras_all),
        'duplicadas':   dups,
        'tiempo':       f"{elapsed:.1f}s",
        'costo':        f"${costo_total:.4f}",
        'tkn_in':       st.session_state['tokens_input'],
        'tkn_out':      st.session_state['tokens_output'],
        'tkn_emb':      st.session_state['tokens_embedding'],
    }
    gc.collect()

# ==============================================================================
# UI PRINCIPAL
# ==============================================================================
def main():
    if not check_password():
        return

    # Header
    st.markdown("""
    <div class="app-header">
        <div class="app-header-icon">🎓</div>
        <div>
            <div class="app-header-title">Sistema de Análisis de Medios · UNAL</div>
            <div class="app-header-sub">
                Limpieza automática · Tono y Tema con IA · Solo para Universidad Nacional de Colombia
            </div>
        </div>
        <div class="app-header-badge">v3.0 · IA</div>
    </div>
    """, unsafe_allow_html=True)

    # ── Sidebar ───────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### 📂 Archivo de entrada")
        dossier_file = st.file_uploader(
            "Dossier principal (.xlsx)",
            type="xlsx",
            help="Excel con las noticias en el formato nuevo (App2)."
        )
        st.markdown("---")
        st.markdown("### ℹ️ Qué hace esta app")
        st.markdown("""
        1. **Limpia** el dossier con la lógica App2 (normalización, dedup avanzado, región, internet map).  
        2. **Analiza tono y tema** con IA únicamente para `Universidad Nacional de Colombia - General`.  
        3. **Otras marcas**: solo limpieza, sin tono/tema.  
        4. **Excel de salida**:  
           - Hoja 1 → **UNAL con IA** (con columnas Tono AI y Tema)  
           - Hoja 2 → **Todas las Marcas** (sin columnas IA)  
        """)
        st.markdown("---")
        st.caption("Configuracion.xlsx debe estar en la raíz del repositorio.")

        st.markdown("---")
        start_btn = st.button(
            "🚀 Iniciar Análisis",
            type="primary",
            use_container_width=True,
            disabled=(dossier_file is None),
        )

    # ── Si se presionó el botón ───────────────────────────────────────────
    if start_btn and dossier_file is not None:
        # Resetear estado
        st.session_state['processing_complete'] = False
        st.session_state['output_data']         = None
        st.session_state['stats']               = {}
        try:
            run_analysis(dossier_file)
        except Exception as e:
            st.error(f"❌ Error durante el análisis: {e}")
            st.exception(e)

    # ── Mostrar resultados ────────────────────────────────────────────────
    if st.session_state.get('processing_complete') and st.session_state.get('output_data'):
        s = st.session_state['stats']

        st.markdown("---")
        st.markdown("### 📊 Resumen del análisis")

        st.markdown(f"""
        <div class="metric-grid">
          <div class="metric-card mc-blue">
            <div class="metric-val" style="color:#003087">{s.get('total', 0):,}</div>
            <div class="metric-lbl">Total filas</div>
          </div>
          <div class="metric-card mc-green">
            <div class="metric-val" style="color:#059669">{s.get('unal_analiz', 0):,}</div>
            <div class="metric-lbl">UNAL analizadas con IA</div>
          </div>
          <div class="metric-card mc-amber">
            <div class="metric-val" style="color:#d97706">{s.get('duplicadas', 0):,}</div>
            <div class="metric-lbl">Duplicadas</div>
          </div>
          <div class="metric-card mc-purple">
            <div class="metric-val" style="color:#7c3aed">{s.get('otras', 0):,}</div>
            <div class="metric-lbl">Otras marcas</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div class="cost-box">
          <span>⏱ <b>Tiempo:</b> {s.get('tiempo', 'N/A')}</span>
          <span>💵 <b>Costo total:</b> {s.get('costo', '$0.00')}</span>
          <span>🔤 <b>Tokens entrada:</b> {s.get('tkn_in', 0):,}</span>
          <span>🔤 <b>Tokens salida:</b> {s.get('tkn_out', 0):,}</span>
          <span>📐 <b>Tokens embedding:</b> {s.get('tkn_emb', 0):,}</span>
        </div>
        """, unsafe_allow_html=True)

        col_dl, col_reset = st.columns([2, 1])
        with col_dl:
            st.download_button(
                label="⬇️ Descargar Informe Excel",
                data=st.session_state['output_data'],
                file_name=st.session_state['output_filename'],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with col_reset:
            if st.button("🔄 Nuevo análisis", use_container_width=True):
                pwd = st.session_state.get("password_correct")
                st.session_state.clear()
                st.session_state["password_correct"] = pwd
                st.rerun()

        st.markdown(f"""
        <div class="info-box">
          📋 El informe contiene:<br>
          &nbsp;&nbsp;• <strong>Hoja 1 "UNAL con IA"</strong>: {s.get('unal', 0):,} filas con columnas <em>Tono AI</em> y <em>Tema</em><br>
          &nbsp;&nbsp;• <strong>Hoja 2 "Todas las Marcas"</strong>: {s.get('total', 0):,} filas totales (sin columnas IA)
        </div>
        """, unsafe_allow_html=True)

    elif not st.session_state.get('processing_complete'):
        st.markdown(
            '<div class="info-box">👈 Carga el dossier en la barra lateral y presiona <strong>Iniciar Análisis</strong>.</div>',
            unsafe_allow_html=True
        )

    st.markdown("---")
    st.markdown(
        "<p style='text-align:center;color:#718096;font-size:0.72rem;font-family:Roboto Mono,monospace;'>"
        "© 2025 Sistema de Análisis UNAL · v3.0 · Desarrollado por Johnathan Cortés</p>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()

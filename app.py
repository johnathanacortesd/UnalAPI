import streamlit as st
import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, Alignment
from collections import defaultdict
import datetime
import io
import re
import json
import time
import gc
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import warnings

import numpy as np
from unidecode import unidecode
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import AgglomerativeClustering
from concurrent.futures import ThreadPoolExecutor, as_completed
from thefuzz import fuzz
from openai import OpenAI

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
# CONFIGURACIÓN DE PÁGINA
# ==============================================================================
st.set_page_config(
    page_title="Análisis UNAL · IA",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Estilos CSS
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

.app-header{
    background:linear-gradient(135deg,#003087 0%,#0050b3 60%,#1a73e8 100%);
    border-radius:var(--r3); padding:1.4rem 2rem; margin-bottom:1.2rem;
    display:flex; align-items:center; gap:1.2rem; position:relative; overflow:hidden;
    box-shadow:0 4px 20px rgba(0,48,135,0.3);
}
.app-header-title{
    font-size:1.35rem; font-weight:700; color:#fff; letter-spacing:-0.01em;
}
.app-header-sub{font-size:0.8rem; color:rgba(255,255,255,0.72); margin-top:0.2rem;}

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

.cost-box{
    background:var(--unal-light); border:1px solid var(--unal-bdr);
    border-radius:var(--r2); padding:0.8rem 1.2rem; margin:0.8rem 0;
    font-family:'Roboto Mono',monospace; font-size:0.78rem; color:var(--unal2);
    display:flex; flex-wrap:wrap; gap:0.5rem 2rem;
}
.cost-box b{color:var(--unal); font-weight:600;}

.success-banner{
    background:var(--green-bg); border:1px solid var(--green-bdr);
    border-left:4px solid var(--green); border-radius:var(--r2);
    padding:0.9rem 1.3rem; margin:0.8rem 0;
    display:flex; align-items:center; gap:0.9rem;
}
.success-title{font-weight:700; color:#047857; font-size:1rem;}
.success-sub{font-size:0.78rem; color:var(--text2);}

.info-box{
    background:var(--unal-light); border-left:3px solid var(--unal);
    border-radius:0 var(--r) var(--r) 0; padding:0.7rem 1rem;
    font-size:0.85rem; color:var(--text2); margin:0.5rem 0;
}

.stButton>button,[data-testid="stDownloadButton"]>button{
    border-radius:100px!important; font-weight:500!important;
}
.stButton>button[kind="primary"],[data-testid="stDownloadButton"]>button{
    background:var(--unal)!important; color:#fff!important;
    border:none!important;
}
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# CONTROL DE ESTADO
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

def check_password() -> bool:
    if st.session_state["password_correct"]:
        return True
    _, col, _ = st.columns([1, 2, 1])
    with col:
        with st.form("pw_form"):
            st.subheader("Ingreso al Sistema")
            pw = st.text_input("Contraseña", type="password")
            if st.form_submit_button("Ingresar", type="primary"):
                correct = st.secrets.get("APP_PASSWORD", "")
                if pw == correct:
                    st.session_state["password_correct"] = True
                    st.rerun()
                else:
                    st.error("Contraseña incorrecta.")
    return False

# ==============================================================================
# PROCESAMIENTO DE TEXTO Y AUXILIARES
# ==============================================================================
def norm_key(text: Any) -> str:
    if text is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

def clean_text(text: Any) -> str:
    if not isinstance(text, str):
        return str(text) if text is not None else ""
    return text.strip()

def texto_para_analisis(titulo: Any, resumen: Any, max_len: int = 2000) -> str:
    t = str(titulo or "").strip()
    r = str(resumen or "").strip()
    return f"TÍTULO: {t}. RESUMEN: {r}"[:max_len]

# ==============================================================================
# CLASIFICACIÓN CON IA (PROMPT & API DE NUEVO FORMATO CLIENTE)
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

def get_embeddings_batch(client: OpenAI, textos: List[str], batch_size: int = 100) -> List[Optional[List[float]]]:
    results: List[Optional[List[float]]] = [None] * len(textos)
    for start in range(0, len(textos), batch_size):
        batch_txt = [t[:2000] for t in textos[start:start + batch_size]]
        try:
            resp = client.embeddings.create(input=batch_txt, model=OPENAI_EMBED)
            st.session_state['tokens_embedding'] += resp.usage.total_tokens
            for j, d in enumerate(resp.data):
                results[start + j] = d.embedding
        except Exception as e:
            st.warning(f"Error en embeddings: {e}")
    return results

def agrupar_noticias_unal(client: OpenAI, rows: List[Dict]) -> List[List[int]]:
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

    # Unión inicial por similitud básica en primeras palabras del título
    titulo_map: Dict[str, int] = {}
    for i, row in enumerate(rows):
        titulo = str(row.get('Título', '') or '')
        tk = norm_key(' '.join(titulo.split()[:4]))
        if tk:
            if tk in titulo_map:
                union(i, titulo_map[tk])
            else:
                titulo_map[tk] = i

    # Agrupación fina mediante embeddings cosine similarity
    textos = [texto_para_analisis(row.get('Título', ''), row.get('Resumen - Aclaracion', '')) for row in rows]
    embs = get_embeddings_batch(client, textos)
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
            grupos_cl = defaultdict(list)
            for k, lbl in enumerate(labels):
                grupos_cl[lbl].append(idxs_v[k])
            for miembros in grupos_cl.values():
                for j in miembros[1:]:
                    union(miembros[0], j)
        except Exception:
            pass

    grupos_finales = defaultdict(list)
    for i in range(n):
        grupos_finales[find(i)].append(i)
    return list(grupos_finales.values())

def _llamar_api_tono_tema(client: OpenAI, texto: str) -> Dict[str, str]:
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
                        "description": "Tono reputacional de la UNAL."
                    },
                    "tema": {
                        "type": "string",
                        "description": "Tema de 4 a 6 palabras que resuma el hecho, sin usar 'UNAL' ni 'Universidad Nacional'."
                    }
                },
                "required": ["tono", "tema"]
            }
        }
    }]
    try:
        resp = client.chat.completions.create(
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
        st.session_state['tokens_input']  += resp.usage.prompt_tokens
        st.session_state['tokens_output'] += resp.usage.completion_tokens
        args = json.loads(resp.choices[0].message.tool_calls[0].function.arguments)
        return {"tono": args.get("tono", "Neutro"), "tema": args.get("tema", "")}
    except Exception:
        return {"tono": "Neutro", "tema": "Error de Clasificación"}

def analizar_tono_tema_unal(client: OpenAI, rows_unal: List[Dict], progress_bar, status_text) -> List[Dict]:
    if not rows_unal:
        return rows_unal

    status_text.markdown('<div class="info-box">🔗 Agrupando noticias similares...</div>', unsafe_allow_html=True)
    progress_bar.progress(0.1, "Agrupando noticias UNAL...")

    grupos = agrupar_noticias_unal(client, rows_unal)
    textos_agrupados = []
    for grupo_idxs in grupos:
        repr_idx = grupo_idxs[0]
        texto = texto_para_analisis(
            rows_unal[repr_idx].get('Título', ''),
            rows_unal[repr_idx].get('Resumen - Aclaracion', '')
        )
        textos_agrupados.append((texto, grupo_idxs))

    status_text.markdown(f'<div class="info-box">🤖 Evaluando {len(textos_agrupados)} clusters con IA...</div>', unsafe_allow_html=True)

    resultados: Dict[int, Dict] = {}
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_TONO) as executor:
        future_to_grupo = {
            executor.submit(_llamar_api_tono_tema, client, t): (t, idxs)
            for t, idxs in textos_agrupados
        }
        for future in as_completed(future_to_grupo):
            _, indices = future_to_grupo[future]
            try:
                res = future.result()
            except Exception:
                res = {"tono": "Neutro", "tema": "Error en cluster"}
            for idx in indices:
                resultados[idx] = res
            completed += 1
            pct = 0.2 + 0.7 * (completed / len(textos_agrupados))
            progress_bar.progress(pct, f"Procesando {completed}/{len(textos_agrupados)} grupos")

    # Mapear los resultados de vuelta a los diccionarios
    for i, row in enumerate(rows_unal):
        r = resultados.get(i, {"tono": "Neutro", "tema": ""})
        row['Tono AI'] = r['tono']
        row['Tema']    = r['tema']

    # Consolidar temas parecidos
    rows_unal = consolidar_temas_similares(rows_unal)
    return rows_unal

def consolidar_temas_similares(rows: List[Dict], umbral: int = 85) -> List[Dict]:
    temas_unicos = list(set(
        row.get('Tema', '') for row in rows if row.get('Tema') and row.get('Tema') not in ('', 'Duplicada')
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

    grupos = defaultdict(list)
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
# CONFIGURACIÓN DE COLUMNAS PARA EXCEL DE SALIDA
# ==============================================================================
COLUMN_ORDER_UNAL = [
    "ID Noticia", "Fecha", "Hora", "Medio", "Tipo de Medio",
    "Sección - Programa", "Región", "Título", "Autor - Conductor",
    "Nro. Pagina", "Dimensión", "Duración - Nro. Caracteres",
    "CPE", "Tier", "Audiencia", "Tono", "Tono AI", "Tema",
    "Link Nota", "Resumen - Aclaracion", "Link (Streaming - Imagen)",
    "Menciones - Empresa", "ID duplicada"
]

COLUMN_ORDER_OTRAS = [
    "ID Noticia", "Fecha", "Hora", "Medio", "Tipo de Medio",
    "Sección - Programa", "Región", "Título", "Autor - Conductor",
    "Nro. Pagina", "Dimensión", "Duración - Nro. Caracteres",
    "CPE", "Tier", "Audiencia", "Tono",
    "Link Nota", "Resumen - Aclaracion", "Link (Streaming - Imagen)",
    "Menciones - Empresa", "ID duplicada"
]

# ==============================================================================
# GENERADOR DE EXCEL CON OPENPYXL
# ==============================================================================
def write_openpyxl_sheet(ws, df: pd.DataFrame, columns: List[str]):
    # Escribir encabezados
    ws.append(columns)
    for cell in ws[1]:
        cell.font = Font(bold=True)

    # Escribir registros
    for _, row in df.iterrows():
        raw_values = []
        hyperlinks_to_add = {}

        for col_idx, col_name in enumerate(columns, start=1):
            val = row.get(col_name) if col_name in df.columns else ""
            
            # Formatear celdas con hipervínculos si contienen URLs
            if isinstance(val, str) and (val.startswith("http://") or val.startswith("https://")):
                raw_values.append("Link")
                hyperlinks_to_add[col_idx] = val
            elif pd.isna(val):
                raw_values.append(None)
            else:
                raw_values.append(val)

        ws.append(raw_values)
        curr_row = ws.max_row

        # Aplicar hipervínculos reales
        for c_idx, url in hyperlinks_to_add.items():
            cell = ws.cell(row=curr_row, column=c_idx)
            cell.hyperlink = url
            cell.font = Font(color="0563C1", underline="single")
            cell.alignment = Alignment(horizontal='left')

    # Ajuste de ancho de columnas básico
    for i, col in enumerate(columns, start=1):
        letter = ws.cell(row=1, column=i).column_letter
        if col in ('Título', 'Resumen - Aclaracion'):
            ws.column_dimensions[letter].width = 45
        else:
            ws.column_dimensions[letter].width = 18

def generate_excel_output(df_unal: pd.DataFrame, df_todas: pd.DataFrame) -> bytes:
    wb = Workbook()
    
    # Pestaña 1: UNAL clasificado
    ws1 = wb.active
    ws1.title = "UNAL con IA"
    write_openpyxl_sheet(ws1, df_unal, COLUMN_ORDER_UNAL)

    # Pestaña 2: Todas las Marcas (Formato Limpio)
    ws2 = wb.create_sheet("Todas las Marcas")
    write_openpyxl_sheet(ws2, df_todas, COLUMN_ORDER_OTRAS)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()

# ==============================================================================
# FLUJO PRINCIPAL
# ==============================================================================
def run_analysis(dossier_file) -> None:
    st.session_state.update({
        'tokens_input': 0, 'tokens_output': 0, 'tokens_embedding': 0
    })

    api_key = st.secrets.get("OPENAI_API_KEY", "")
    if not api_key:
        st.error("🔑 No se encontró la credencial 'OPENAI_API_KEY' en los secretos de Streamlit.")
        st.stop()
    client = OpenAI(api_key=api_key)

    t0 = time.time()
    progress_bar = st.progress(0, "Iniciando análisis...")
    status_text = st.empty()

    # Fase 1: Carga y segmentación de datos limpios
    status_text.markdown('<div class="info-box">📁 Cargando el dataset pre-limpiado...</div>', unsafe_allow_html=True)
    df = pd.read_excel(dossier_file)

    # Identificar filas asignadas a la marca UNAL
    filas_unal_mask = df["Menciones - Empresa"].astype(str).str.strip() == UNAL_BRAND
    df_unal = df[filas_unal_mask].copy()
    df_otras = df.copy() # Conserva el total en la pestaña general

    total_rows = len(df)
    unal_rows = len(df_unal)
    otras_rows = total_rows - unal_rows

    if unal_rows > 0:
        # Preparación de las columnas requeridas para IA
        df_unal["Tono AI"] = ""
        df_unal["Tema"] = ""

        # Separar registros únicos de duplicados para optimizar llamadas
        es_duplicada_mask = df_unal["ID duplicada"].fillna("").astype(str) != ""
        df_unal_no_dups = df_unal[~es_duplicada_mask].copy()
        df_unal_dups = df_unal[es_duplicada_mask].copy()

        if len(df_unal_dups) > 0:
            df_unal_dups["Tono AI"] = "Duplicada"
            df_unal_dups["Tema"] = "Duplicada"

        if len(df_unal_no_dups) > 0:
            dict_list_no_dups = df_unal_no_dups.to_dict(orient="records")
            analizados_dict = analizar_tono_tema_unal(client, dict_list_no_dups, progress_bar, status_text)
            df_unal_no_dups_final = pd.DataFrame(analizados_dict)
        else:
            df_unal_no_dups_final = pd.DataFrame()

        # Combinar de nuevo filas únicas y duplicadas
        df_unal_final = pd.concat([df_unal_no_dups_final, df_unal_dups], ignore_index=True)
    else:
        df_unal_final = pd.DataFrame(columns=COLUMN_ORDER_UNAL)

    progress_bar.progress(0.9, "Generando archivo Excel de salida...")
    output_bytes = generate_excel_output(df_unal_final, df_otras)

    elapsed_time = time.time() - t0
    cost_total = (
        (st.session_state['tokens_input'] / 1e6) * PRICE_INPUT_1M +
        (st.session_state['tokens_output'] / 1e6) * PRICE_OUTPUT_1M +
        (st.session_state['tokens_embedding'] / 1e6) * PRICE_EMBEDDING_1M
    )

    progress_bar.progress(1.0, "Listo")
    status_text.markdown(
        '<div class="success-banner">'
        '<div class="success-icon">✓</div>'
        '<div><div class="success-title">Proceso finalizado con éxito</div>'
        '<div class="success-sub">El archivo estructurado y clasificado se encuentra listo para descargar.</div></div>'
        '</div>',
        unsafe_allow_html=True
    )

    st.session_state['processing_complete'] = True
    st.session_state['output_data'] = output_bytes
    st.session_state['output_filename'] = f"Informe_Analizado_UNAL_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    st.session_state['stats'] = {
        'total': total_rows,
        'unal': unal_rows,
        'otras': otras_rows,
        'tiempo': f"{elapsed_time:.1f}s",
        'costo': f"${cost_total:.4f}",
        'tkn_in': st.session_state['tokens_input'],
        'tkn_out': st.session_state['tokens_output'],
        'tkn_emb': st.session_state['tokens_embedding'],
    }

# ==============================================================================
# VISTA GENERAL DE LA INTERFAZ
# ==============================================================================
def main():
    if not check_password():
        return

    st.markdown("""
    <div class="app-header">
        <div class="app-header-title">Sistema de Clasificación Temática · UNAL</div>
        <div class="app-header-sub">
            Evaluación inteligente de Tono y Temas de opinión mediante GPT para la Universidad Nacional de Colombia.
        </div>
    </div>
    """, unsafe_allow_html=True)

    with st.sidebar:
        st.markdown("### 📂 Carga de Entrada")
        dossier_file = st.file_uploader(
            "Archivo Limpio (.xlsx)",
            type="xlsx",
            help="Carga el archivo Excel que ya ha pasado por tu proceso de limpieza."
        )
        st.markdown("---")
        start_btn = st.button("🚀 Procesar con IA", type="primary", use_container_width=True, disabled=(dossier_file is None))

    if start_btn and dossier_file is not None:
        st.session_state['processing_complete'] = False
        st.session_state['output_data'] = None
        st.session_state['stats'] = {}
        try:
            run_analysis(dossier_file)
        except Exception as e:
            st.error(f"Se produjo un inconveniente técnico durante el análisis: {e}")
            st.exception(e)

    if st.session_state.get('processing_complete') and st.session_state.get('output_data'):
        s = st.session_state['stats']

        st.markdown("### 📊 Métricas del Documento")
        st.markdown(f"""
        <div class="metric-grid">
          <div class="metric-card mc-blue">
            <div class="metric-val" style="color:#003087">{s.get('total', 0):,}</div>
            <div class="metric-lbl">Total Registros</div>
          </div>
          <div class="metric-card mc-green">
            <div class="metric-val" style="color:#059669">{s.get('unal', 0):,}</div>
            <div class="metric-lbl">Menciones UNAL (IA)</div>
          </div>
          <div class="metric-card mc-purple">
            <div class="metric-val" style="color:#7c3aed">{s.get('otras', 0):,}</div>
            <div class="metric-lbl">Otras Marcas</div>
          </div>
          <div class="metric-card mc-amber">
            <div class="metric-val" style="color:#d97706">{s.get('tiempo', 'N/A')}</div>
            <div class="metric-lbl">Tiempo de Ejecución</div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div class="cost-box">
          <span>💵 <b>Inversión estimada API:</b> {s.get('costo', '$0.00')}</span>
          <span>🔤 <b>Tokens entrada:</b> {s.get('tkn_in', 0):,}</span>
          <span>🔤 <b>Tokens salida:</b> {s.get('tkn_out', 0):,}</span>
          <span>📐 <b>Tokens de embedding:</b> {s.get('tkn_emb', 0):,}</span>
        </div>
        """, unsafe_allow_html=True)

        col_dl, col_reset = st.columns([2, 1])
        with col_dl:
            st.download_button(
                label="⬇️ Descargar Reporte Completo (Excel)",
                data=st.session_state['output_data'],
                file_name=st.session_state['output_filename'],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with col_reset:
            if st.button("🔄 Cargar nuevo archivo", use_container_width=True):
                pwd = st.session_state.get("password_correct")
                st.session_state.clear()
                st.session_state["password_correct"] = pwd
                st.rerun()
    else:
        st.info("Por favor, sube el archivo Excel limpio utilizando la barra lateral y presiona 'Procesar con IA'.")

if __name__ == "__main__":
    main()

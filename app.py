# ==============================================================================
# ANÁLISIS DE TONO Y TEMA PARA UNIVERSIDAD NACIONAL - APP STREAMLIT (OPTIMIZADO)
# ==============================================================================
import streamlit as st
import pandas as pd
from openpyxl import load_workbook, Workbook
from collections import defaultdict
from copy import deepcopy
import datetime
import io
import re
import json
import time
from unidecode import unidecode
from typing import List, Dict, Any, Tuple
from tqdm import tqdm
import warnings
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
warnings.filterwarnings('ignore')
# ==============================================================================
# CONFIGURACIÓN DE LA PÁGINA DE STREAMLIT
# ==============================================================================
st.set_page_config(
    page_title="Análisis de Tono y Tema UNAL",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded"
)
# CSS personalizado para mejorar la interfaz
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #1e3a8a 0%, #3b82f6 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 30px;
    }
    .metric-card {
        background: #f8fafc;
        padding: 20px;
        border-radius: 10px;
        border-left: 4px solid #3b82f6;
        margin: 10px 0;
    }
    .success-box {
        background: #dcfce7;
        border-left: 4px solid #22c55e;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .info-box {
        background: #dbeafe;
        border-left: 4px solid #3b82f6;
        padding: 15px;
        border-radius: 5px;
        margin: 10px 0;
    }
    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #3b82f6 0%, #8b5cf6 100%);
    }
</style>
""", unsafe_allow_html=True)
# ==============================================================================
# FUNCIÓN DE AUTENTICACIÓN
# ==============================================================================
def check_password():
    """Devuelve `True` si el usuario ha introducido la contraseña correcta."""
    if "password_correct" in st.session_state and st.session_state["password_correct"]:
        return True
    st.markdown('<div class="main-header"><h1>🔐 Acceso Protegido</h1><p>Sistema de Análisis de Medios - UNAL</p></div>', unsafe_allow_html=True)
   
    with st.form("password_form"):
        st.markdown("Por favor, introduce la contraseña para acceder a la aplicación.")
        password = st.text_input("Contraseña", type="password", placeholder="Ingrese la contraseña")
        submitted = st.form_submit_button("🚀 Ingresar", use_container_width=True)
        if submitted:
            correct_password = st.secrets.get("APP_PASSWORD")
            if not correct_password:
                st.error("❌ Error de configuración: No se ha establecido una contraseña para la aplicación.")
                return False
           
            if password == correct_password:
                st.session_state["password_correct"] = True
                st.rerun()
            else:
                st.error("❌ La contraseña es incorrecta.")
    return False
# ==============================================================================
# FUNCIONES OPTIMIZADAS CON CACHE
# ==============================================================================
@lru_cache(maxsize=10000)
def norm_key(text: Any) -> str:
    if text is None: return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))
@lru_cache(maxsize=5000)
def corregir_texto(text: Any) -> str:
    if not isinstance(text, str): return ""
    text = re.sub(r'(<br\s*/?>|\[\.\.\.\])+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    match = re.search(r"[A-ZÁÉÍÓÚÑ]", text)
    if match: text = text[match.start():]
    return text
def clean_title_for_output(title: Any) -> str:
    if not isinstance(title, str): return str(title if title is not None else "")
    return re.sub(r"\s*\|\s*[\w\s]+$", "", title).strip()
@lru_cache(maxsize=100)
def normalizar_tipo_medio(tipo_raw: str) -> str:
    if not isinstance(tipo_raw, str): return str(tipo_raw)
    t = unidecode(str(tipo_raw).strip().lower())
    mapping = {
        "fm": "Radio", "am": "Radio", "radio": "Radio", "aire": "Televisión", "cable": "Televisión", "tv": "Televisión",
        "television": "Televisión", "televisión": "Televisión", "senal abierta": "Televisión", "señal abierta": "Televisión",
        "diario": "Prensa", "prensa": "Prensa", "revista": "Revista", "revistas": "Revista", "online": "Internet",
        "internet": "Internet", "digital": "Internet", "web": "Internet"
    }
    return mapping.get(t, str(tipo_raw).strip().title() if str(tipo_raw).strip() else "Otro")
def extract_link(cell):
    if hasattr(cell, "hyperlink") and cell.hyperlink and cell.hyperlink.target:
        return {"value": cell.value or "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        match = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if match: return {"value": "Link", "url": match.group(1)}
    return {"value": cell.value, "url": None}
def run_base_logic(sheet, progress_hook):
    headers = [c.value for c in sheet[1] if c.value]
    norm_keys = [norm_key(h) for h in headers]
    key_map = {nk: nk for nk in norm_keys}
    key_map.update({
        "titulo": norm_key("Titulo"), "resumen": norm_key("Resumen - Aclaracion"), "menciones": norm_key("Menciones - Empresa"),
        "medio": norm_key("Medio"), "tonoai": norm_key("Tono AI"), "tema": norm_key("Tema"), "idnoticia": norm_key("ID Noticia"),
        "idduplicada": norm_key("ID duplicada"), "tipodemedio": norm_key("Tipo de Medio"), "link_nota": norm_key("Link Nota"),
        "link_streaming": norm_key("Link (Streaming - Imagen)"), "region": norm_key("Region"), "hora": norm_key("Hora")
    })
    rows = [{norm_keys[i]: cell for i, cell in enumerate(row) if i < len(norm_keys)}
            for row in sheet.iter_rows(min_row=2) if not all(c.value is None for c in row)]
    split_rows = []
    for r_cells in rows:
        base = {k: extract_link(v) if k in [key_map["link_nota"], key_map["link_streaming"]] else v.value for k, v in r_cells.items()}
        base[key_map["tipodemedio"]] = normalizar_tipo_medio(base.get(key_map["tipodemedio"]))
        menciones_raw = str(base.get(key_map["menciones"], ""))
        menciones_list = [m.strip() for m in menciones_raw.split(";") if m.strip()] or [menciones_raw]
        for m in menciones_list:
            new = deepcopy(base)
            new[key_map["menciones"]] = m
            split_rows.append(new)
    for idx, row in enumerate(split_rows):
        row.update({"original_index": idx, "is_duplicate": False, "tonoai": "", "tema": ""})
   
    processed_rows = detectar_duplicados_avanzado(split_rows, key_map, progress_hook)
   
    for row in processed_rows:
        if row["is_duplicate"]:
            row.update({key_map["tonoai"]: "Duplicada", key_map["tema"]: "Duplicada"})
    return processed_rows, key_map
def detectar_duplicados_avanzado(rows: List[Dict], key_map: Dict[str, str], progress_hook):
    processed_rows = deepcopy(rows)
    seen_text_key, seen_online_url, seen_broadcast = {}, {}, {}
    id_key, id_duplicada_key = key_map.get("idnoticia", "idnoticia"), key_map.get("idduplicada", "idduplicada")
    titulo_key, resumen_key = key_map.get("titulo", "titulo"), key_map.get("resumen", "resumen")
    mencion_key, medio_key = key_map.get("menciones", "menciones"), key_map.get("medio", "medio")
    tipo_medio_key, link_nota_key, hora_key = key_map.get("tipodemedio"), key_map.get("link_nota"), key_map.get("hora")
   
    total_rows = len(processed_rows)
    for i, row in enumerate(processed_rows):
        if i % 100 == 0:
            progress_hook(i, total_rows, "Detectando duplicados...")
        mencion_norm, medio_norm = norm_key(row.get(mencion_key)), norm_key(row.get(medio_key))
        texto_titulo, texto_resumen = corregir_texto(row.get(titulo_key, '')), corregir_texto(row.get(resumen_key, ''))
        texto_base = texto_titulo if texto_titulo else texto_resumen
        if texto_base:
            clave_texto_norm = norm_key(" ".join(texto_base.split()[:3]))
            key = (clave_texto_norm, mencion_norm, medio_norm)
            if clave_texto_norm and key in seen_text_key:
                row["is_duplicate"] = True
                row[id_duplicada_key] = processed_rows[seen_text_key[key]].get(id_key, "")
                continue
            else: seen_text_key[key] = i
        tipo_medio = normalizar_tipo_medio(str(row.get(tipo_medio_key)))
        if tipo_medio == "Internet":
            url = (row.get(link_nota_key, {}) or {}).get("url")
            if url and mencion_norm:
                key_url = (url, mencion_norm)
                if key_url in seen_online_url:
                    row["is_duplicate"] = True
                    row[id_duplicada_key] = processed_rows[seen_online_url[key_url]].get(id_key, "")
                else: seen_online_url[key_url] = i
        elif tipo_medio in ["Radio", "Televisión"]:
            hora = str(row.get(hora_key, "")).strip()
            if mencion_norm and medio_norm and hora:
                key_broadcast = (mencion_norm, medio_norm, hora)
                if key_broadcast in seen_broadcast:
                    row["is_duplicate"] = True
                    row[id_duplicada_key] = processed_rows[seen_broadcast[key_broadcast]].get(id_key, "")
                else: seen_broadcast[key_broadcast] = i
    return processed_rows
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
    return all_processed_rows
def process_link_logic(all_rows, key_map):
    ln_key, ls_key = key_map.get("link_nota"), key_map.get("link_streaming")
    for row in all_rows:
        tipo = normalizar_tipo_medio(row.get(key_map.get("tipodemedio"), ""))
        ln, ls = row.get(ln_key) or {}, row.get(ls_key) or {}
        has_url = lambda x: isinstance(x, dict) and bool(x.get("url"))
        if tipo in ["Radio", "Televisión"]: row[ls_key] = None
        elif tipo == "Internet": row[ln_key], row[ls_key] = ls, ln
        elif tipo in ["Prensa", "Revista"]:
            if not has_url(ln) and has_url(ls): row[ln_key] = ls
            row[ls_key] = None
    return all_rows
def process_sov_mapping_final(all_rows: List[Dict], key_map: Dict[str, str], sov_file):
    df_sov = pd.read_excel(sov_file)
    cols_by_norm = {norm_key(c): c for c in df_sov.columns}
    menc_col, name_col = cols_by_norm.get(norm_key("Menciones - Empresa")), cols_by_norm.get(norm_key("Nombre"))
    if not menc_col or not name_col: return all_rows
    sov_map = {str(r.get(menc_col, "")).strip().lower(): str(r.get(name_col)).strip() for _, r in df_sov.iterrows() if str(r.get(menc_col, "")).strip() and str(r.get(name_col, "")).strip()}
    for r in all_rows:
        mk = str(r.get(key_map.get("menciones"), "")).strip().lower()
        if mk in sov_map: r[key_map.get("menciones")] = sov_map[mk]
    return all_rows
def _append_rows_to_sheet(sheet, rows_data, key_map, include_ai_columns):
    base_order = ["ID Noticia", "Fecha", "Hora", "Medio", "Tipo de Medio", "Seccion - Programa", "Region", "Titulo", "Autor - Conductor", "Nro. Pagina", "Dimension", "Duracion - Nro. Caracteres", "CPE", "Tier", "Audiencia", "Tono", "Resumen - Aclaracion", "Link Nota", "Link (Streaming - Imagen)", "Menciones - Empresa", "ID duplicada"]
    ai_order = ["Tono AI", "Tema"]
    final_order = base_order[:16] + ai_order + base_order[16:] if include_ai_columns else base_order
    sheet.append(final_order)
    for row_data in rows_data:
        row_data[key_map.get("titulo")] = clean_title_for_output(row_data.get(key_map.get("titulo")))
        row_data[key_map.get("resumen")] = corregir_texto(str(row_data.get(key_map.get("resumen"), ""))).replace("_x000D_", "")
        row_to_append, links_to_add = [], {}
        for col_idx, header in enumerate(final_order, 1):
            val = row_data.get(norm_key(header))
            cell_value = None
            if isinstance(val, dict) and val.get("url"):
                cell_value, url = "Link", val.get("url")
                links_to_add[col_idx] = url
            elif val is not None: cell_value = str(val)
            row_to_append.append(cell_value)
        sheet.append(row_to_append)
        for col_idx, url in links_to_add.items():
            cell = sheet.cell(row=sheet.max_row, column=col_idx)
            cell.hyperlink = url
            cell.style = "Hyperlink"
def generate_excel_output(all_processed_rows, key_map):
    out_wb = Workbook()
    sheet1 = out_wb.active
    sheet1.title = "UNAL con IA"
    unal_rows = [row for row in all_processed_rows if row.get("__is_target_brand")]
    _append_rows_to_sheet(sheet1, unal_rows, key_map, include_ai_columns=True)
    sheet2 = out_wb.create_sheet("Todas las Marcas")
    _append_rows_to_sheet(sheet2, all_processed_rows, key_map, include_ai_columns=False)
    output_buffer = io.BytesIO()
    out_wb.save(output_buffer)
    output_buffer.seek(0)
    return output_buffer
# <<< MEJORA CRÍTICA: Lógica de agrupación completamente reescrita con Union-Find para máxima robustez.
def agrupar_noticias_similares(rows: List[Dict], key_map: Dict[str, str]) -> List[List[int]]:
    """
    Agrupa noticias utilizando un algoritmo Union-Find. Este método garantiza que si la noticia A
    es similar a B (por título o resumen) y B es similar a C, entonces A, B y C terminarán en
    el mismo grupo, resolviendo problemas de agrupación transitiva de forma eficiente.
    """
    titulo_key, resumen_key = key_map.get("titulo", "titulo"), key_map.get("resumen", "resumen")
    n = len(rows)
    parent = list(range(n))
    def find(i):
        if parent[i] == i:
            return i
        parent[i] = find(parent[i])
        return parent[i]
    def union(i, j):
        root_i = find(i)
        root_j = find(j)
        if root_i != root_j:
            parent[root_j] = root_i
    titulo_a_raiz = {}
    resumen_a_raiz = {}
    for idx, row in enumerate(rows):
        # Generar clave para el título (primeras 4 palabras)
        titulo = corregir_texto(row.get(titulo_key, ''))
        if titulo:
            clave_titulo = norm_key(" ".join(titulo.strip().split()[:4]))
            if clave_titulo:
                if clave_titulo in titulo_a_raiz:
                    union(idx, titulo_a_raiz[clave_titulo])
                titulo_a_raiz[clave_titulo] = find(idx)
        # Generar clave para el resumen (primeras 6 palabras)
        resumen = corregir_texto(row.get(resumen_key, ''))
        if resumen:
            clave_resumen = norm_key(" ".join(resumen.strip().split()[:6]))
            if clave_resumen:
                if clave_resumen in resumen_a_raiz:
                    union(idx, resumen_a_raiz[clave_resumen])
                resumen_a_raiz[clave_resumen] = find(idx)
    # Consolidar los grupos finales
    grupos_finales = defaultdict(list)
    for i in range(n):
        grupos_finales[find(i)].append(i)
    return list(grupos_finales.values())
class CostTracker:
    def __init__(self, limit_usd: float, input_cost_per_1m: float, output_cost_per_1m: float):
        self.limit_usd, self.total_cost = limit_usd, 0.0
        self.input_cost_per_token, self.output_cost_per_token = input_cost_per_1m / 1e6, output_cost_per_1m / 1e6
        self.total_input_tokens, self.total_output_tokens = 0, 0
    def add_cost(self, input_tokens: int, output_tokens: int):
        cost = (input_tokens * self.input_cost_per_token) + (output_tokens * self.output_cost_per_token)
        self.total_cost += cost
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens
    def is_limit_exceeded(self) -> bool: return self.total_cost >= self.limit_usd
    def get_summary(self) -> Dict:
        remaining = max(0, self.limit_usd - self.total_cost)
        return {
            "limit": self.limit_usd,
            "total_cost": self.total_cost,
            "input_tokens": self.total_input_tokens,
            "output_tokens": self.total_output_tokens,
            "remaining": remaining
        }
def analizar_con_openai_parallel(textos_agrupados: List[Tuple[str, List[int]]], cost_tracker: CostTracker, client: OpenAI, progress_hook, max_workers: int = 3):
    resultados = {}
    tools = [{"type": "function", "function": {"name": "clasificar_noticia_unal", "description": "Clasifica el tono y el tema de una noticia sobre la Universidad Nacional.", "parameters": {"type": "object", "properties": {"tono": {"type": "string", "description": "El tono de la noticia: Positivo, Negativo o Neutro.", "enum": ["Positivo", "Negativo", "Neutro"]}, "tema": {"type": "string", "description": "Tema específico de 4 a 6 palabras que resume el hecho principal. No debe incluir el nombre de la universidad ni ser genérico."}}, "required": ["tono", "tema"]}}}]
   
    system_prompt = """Eres un analista de medios hiper-especializado y tu única misión es evaluar el impacto de las noticias sobre la Universidad Nacional de Colombia (UNAL), enfocándote exclusivamente en la afectación o gestión de la universidad. Debes ser implacable en la aplicación de las siguientes reglas, considerando el contexto completo y cómo la noticia afecta directamente la reputación, operaciones o gestión de la UNAL.
**REGLA DE ORO INQUEBRANTABLE:**
- Si la UNAL **NO es el actor principal** de la noticia, o si su mención es meramente contextual, referencial, o como fuente de opinión, el tono es **SIEMPRE NEUTRO**. No importa si la noticia trata sobre violencia, política o crisis; si no afecta directamente la gestión o reputación de la UNAL, es NEUTRO.
**REGLAS DE TONO (SÓLO si la UNAL es el actor principal y la noticia afecta su gestión o reputación):**
- **NEGATIVO:** Únicamente si la noticia reporta un **fallo directo de la UNAL** o un **evento perjudicial que ocurre bajo su responsabilidad directa**. Ejemplos:
  - Críticas a la gestión de directivas de la UNAL.
  - Disturbios violentos DENTRO de un campus de la UNAL.
  - Escándalos de corrupción o acoso que involucren a la UNAL.
  - Resultados negativos en rankings o acreditaciones.
- **POSITIVO:** Únicamente si la noticia reporta un **logro o acción destacada de la UNAL**. Ejemplos:
  - Premios o reconocimientos a la UNAL, sus profesores o estudiantes.
  - Avances científicos o proyectos de impacto liderados por la UNAL.
  - Buena gestión, convenios exitosos, aumento de presupuesto.
  - Contribuciones significativas de la UNAL a la sociedad.
- **NEUTRO:** Para **todo lo demás**, incluyendo:
  - Anuncios de eventos, conferencias o procesos de admisión.
  - Menciones de la UNAL en noticias sobre el sector educativo general.
  - Opiniones de expertos de la UNAL sobre temas externos.
  - Hechos que ocurren cerca de un campus pero sin relación directa con la universidad.
**REGLAS CRÍTICAS PARA EL TEMA:**
1. **HECHO PRINCIPAL:** Describe el evento central del grupo de noticias.
2. **SIN MARCA:** **NO** incluyas "Universidad Nacional", "UNAL", etc.
3. **LONGITUD ESTRICTA:** Debe tener **entre 4 y 6 palabras.**
Tu precisión es vital. Evalúa el impacto, no el sentimiento general."""
   
    def procesar_grupo(i, texto_representativo, indices_grupo):
        if cost_tracker.is_limit_exceeded():
            return None
        try:
            response = client.chat.completions.create(
               model="gpt-4.1-nano-2025-04-14",
               messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": f"Analiza este grupo de noticias: \"{texto_representativo}\""}],
               tools=tools,
               tool_choice={"type": "function", "function": {"name": "clasificar_noticia_unal"}},
               temperature=0.0,
               max_tokens=250
            )
            if response.usage: cost_tracker.add_cost(response.usage.prompt_tokens, response.usage.completion_tokens)
            resultado_json = json.loads(response.choices[0].message.tool_calls[0].function.arguments)
            return (indices_grupo, resultado_json["tono"], resultado_json["tema"])
        except Exception as e:
            st.warning(f"Advertencia en API para grupo {i + 1}: {e}. Asignando error.")
            return (indices_grupo, "Error", "Excepción API")
   
    total_grupos = len(textos_agrupados)
    completed = 0
   
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(procesar_grupo, i, texto, indices): i for i, (texto, indices) in enumerate(textos_agrupados)}
       
        for future in as_completed(futures):
            result = future.result()
            if result:
                indices_grupo, tono, tema = result
                for idx in indices_grupo:
                    resultados[idx] = {"tono": tono, "tema": tema}
            completed += 1
            progress_hook(completed, total_grupos, f"Analizando grupos con IA ({completed}/{total_grupos})...")
   
    return resultados
def procesar_global(target_rows: List[Dict], key_map: Dict[str, str], cost_tracker: CostTracker, client: OpenAI, status_placeholder, progress_bar):
    total_rows = len(target_rows)
    titulo_key, resumen_key = key_map.get("titulo"), key_map.get("resumen")
    tono_key, tema_key = key_map.get("tonoai"), key_map.get("tema")
   
    status_placeholder.markdown('<div class="info-box">📦 <strong>Procesando todas las noticias UNAL</strong> ({total_rows:,} noticias)</div>', unsafe_allow_html=True)
   
    grupos_fusionados = agrupar_noticias_similares(target_rows, key_map)
   
    textos_agrupados = []
    for indices_locales in grupos_fusionados:
        if not indices_locales: continue
        idx_repr = indices_locales[0]
        texto_completo = f"TÍTULO: {corregir_texto(target_rows[idx_repr].get(titulo_key, ''))}. RESUMEN: {corregir_texto(target_rows[idx_repr].get(resumen_key, ''))}".strip()[:3500]
        if texto_completo and texto_completo != "TÍTULO: . RESUMEN: ":
            textos_agrupados.append((texto_completo, indices_locales))
   
    def progress_hook_ia(current, total, text):
        progress = (current / total) * 0.33 if total > 0 else 0
        progress_bar.progress(0.33 + progress, text=f"🤖 {text}")
    resultados = analizar_con_openai_parallel(textos_agrupados, cost_tracker, client, progress_hook_ia, max_workers=3)
   
    for idx, resultado in resultados.items():
        if idx < len(target_rows):
            target_rows[idx][tono_key] = resultado["tono"]
            target_rows[idx][tema_key] = resultado["tema"]
   
    return target_rows
# ==============================================================================
# LÓGICA PRINCIPAL DE LA APLICACIÓN
# ==============================================================================
if not check_password():
    st.stop()
api_key = st.secrets.get("OPENAI_API_KEY")
if not api_key:
    st.error("❌ Error de Configuración: La API Key de OpenAI no está configurada.")
    st.stop()
# Header principal
st.markdown('<div class="main-header"><h1>🎓 Sistema de Análisis de Medios UNAL</h1><p>Análisis Inteligente de Tono y Tema con IA | Desarrollado por Johnathan Cortés ©️</p></div>', unsafe_allow_html=True)
if 'analysis_done' not in st.session_state: st.session_state.analysis_done = False
if 'result_buffer' not in st.session_state: st.session_state.result_buffer = None
if 'final_summary' not in st.session_state: st.session_state.final_summary = {}
if 'analysis_stats' not in st.session_state: st.session_state.analysis_stats = {}
with st.sidebar:
    st.markdown("### 📂 Carga de Archivos")
    dossier_file = st.file_uploader("Dossier Principal", type="xlsx", help="Archivo principal con las noticias")
    region_file = st.file_uploader("Mapeo de Región", type="xlsx", help="Relación Medio-Región")
    internet_file = st.file_uploader("Mapeo de Internet", type="xlsx", help="Relación Medio-Internet")
    sov_file = st.file_uploader("Mapeo SOV", type="xlsx", help="Relación Menciones-Marca")
   
    st.markdown("---")
    st.markdown("### ⚙️ Configuración")
   
    col1, col2 = st.columns(2)
    with col1:
        cost_limit_usd = st.number_input("💰 Límite (USD)", min_value=0.10, max_value=10.0, value=1.00, step=0.10)
    with col2:
        batch_size = st.slider("📦 Lote", min_value=100, max_value=1000, value=400, step=50)
   
    st.markdown("---")
    all_files_ready = all([dossier_file, region_file, internet_file, sov_file])
   
    if not all_files_ready:
        st.warning("⚠️ Cargue todos los archivos para continuar")
   
    start_button = st.button("🚀 Iniciar Análisis", type="primary", use_container_width=True, disabled=(not all_files_ready))
if start_button:
    st.session_state.analysis_done = False
    st.session_state.result_buffer = None
    st.session_state.analysis_stats = {}
    client = OpenAI(api_key=api_key)
    TARGET_BRANDS = ["U. Nacional de Colombia", "Universidad Nacional de Colombia", "Universidad Nacional de Colombia - General"]
   
    progress_bar = st.progress(0, text="🚀 Iniciando proceso...")
    status_container = st.empty()
    metrics_container = st.container()
    def main_progress_hook(current, total, text):
        if total > 0: progress_bar.progress(min((current / total) * 0.33, 0.32), text=text)
    start_time = time.time()
    try:
        # FASE 1: Preparación de datos
        status_container.markdown('<div class="info-box">📋 <strong>Fase 1/3:</strong> Preparando y limpiando datos...</div>', unsafe_allow_html=True)
        all_processed_rows, key_map = run_base_logic(load_workbook(dossier_file, data_only=True).active, main_progress_hook)
        all_processed_rows = process_mappings_and_links(all_processed_rows, key_map, region_file, internet_file)
        all_processed_rows = process_link_logic(all_processed_rows, key_map)
        for row in all_processed_rows: row["__is_target_brand"] = (row.get(key_map.get("menciones")) in TARGET_BRANDS)
       
        total_news = len(all_processed_rows)
        unal_news = sum(1 for r in all_processed_rows if r.get('__is_target_brand'))
        duplicates = sum(1 for r in all_processed_rows if r.get('is_duplicate'))
       
        with metrics_container:
            col1, col2, col3 = st.columns(3)
            col1.metric("📰 Total Noticias", f"{total_news:,}")
            col2.metric("🎓 Noticias UNAL", f"{unal_news:,}")
            col3.metric("🔄 Duplicadas", f"{duplicates:,}")
       
        progress_bar.progress(0.33, text="✅ Fase 1 completada")
        # FASE 2: Análisis con IA
        status_container.markdown('<div class="info-box">🤖 <strong>Fase 2/3:</strong> Analizando con Inteligencia Artificial...</div>', unsafe_allow_html=True)
        cost_tracker = CostTracker(cost_limit_usd, 0.10, 0.40)
        target_rows = [row for row in all_processed_rows if row.get("__is_target_brand") and not row.get("is_duplicate")]
       
        status_placeholder = st.empty()
        if target_rows:
            target_rows_procesados = procesar_global(target_rows, key_map, cost_tracker, client, status_placeholder, progress_bar)
            update_map = {row['original_index']: row for row in target_rows_procesados}
            for row in all_processed_rows:
                if row['original_index'] in update_map:
                    row[key_map.get("tonoai")] = update_map[row['original_index']].get(key_map.get("tonoai"))
                    row[key_map.get("tema")] = update_map[row['original_index']].get(key_map.get("tema"))
            st.session_state.final_summary = cost_tracker.get_summary()
            st.session_state.analysis_stats = {
                "processed": len(target_rows),
                "time": time.time() - start_time
            }
       
        progress_bar.progress(0.66, text="✅ Fase 2 completada")
        # FASE 3: Generación de informe
        status_container.markdown('<div class="info-box">📄 <strong>Fase 3/3:</strong> Generando informe final...</div>', unsafe_allow_html=True)
        final_rows = process_sov_mapping_final(all_processed_rows, key_map, sov_file)
        st.session_state.result_buffer = generate_excel_output(final_rows, key_map)
        progress_bar.progress(1.0, text="✅ ¡Proceso completado!")
        st.session_state.analysis_done = True
       
        status_container.markdown('<div class="success-box">🎉 <strong>¡Análisis completado exitosamente!</strong></div>', unsafe_allow_html=True)
    except Exception as e:
        st.error(f"❌ Error durante el proceso: {e}")
        st.exception(e)
# Mostrar resultados
if st.session_state.analysis_done and st.session_state.result_buffer:
    st.markdown("---")
   
    col1, col2 = st.columns([2, 1])
   
    with col1:
        st.markdown("### 📊 Resumen del Análisis")
        summary = st.session_state.get('final_summary', {})
        stats = st.session_state.get('analysis_stats', {})
       
        total_cost = summary.get('total_cost', 0)
        limit = summary.get('limit', 0)
        remaining = max(0, limit - total_cost)
        input_tokens = summary.get('input_tokens', 0)
        output_tokens = summary.get('output_tokens', 0)
        total_tokens = input_tokens + output_tokens
        processing_time = stats.get('time', 0)
        processed_news = stats.get('processed', 0)
       
        metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
        metrics_col1.metric("💵 Costo Total", f"${total_cost:.4f}")
        metrics_col2.metric("💰 Restante", f"${remaining:.4f}")
        metrics_col3.metric("🔤 Tokens", f"{total_tokens:,}")
        metrics_col4.metric("⏱️ Tiempo", f"{processing_time:.1f}s")
       
        st.markdown(f"""
        <div class="metric-card">
            <h4>📈 Detalles de Procesamiento</h4>
            <ul>
                <li><strong>Noticias Analizadas:</strong> {processed_news:,}</li>
                <li><strong>Tokens de Entrada:</strong> {input_tokens:,}</li>
                <li><strong>Tokens de Salida:</strong> {output_tokens:,}</li>
                <li><strong>Límite Establecido:</strong> ${limit:.2f}</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
   
    with col2:
        st.markdown("### 📥 Descargar Informe")
        st.download_button(
            label="⬇️ Descargar Excel",
            data=st.session_state.result_buffer,
            file_name=f"Informe_UNAL_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
       
        st.markdown("""
        <div class="success-box" style="margin-top: 20px;">
            <strong>✅ Archivo Generado</strong><br>
            El informe incluye:<br>
            • Hoja 1: UNAL con IA<br>
            • Hoja 2: Todas las Marcas
        </div>
        """, unsafe_allow_html=True)
else:
    st.markdown('<div class="info-box">👈 Configure los parámetros en la barra lateral y presione <strong>"Iniciar Análisis"</strong> para comenzar.</div>', unsafe_allow_html=True)
# Footer
st.markdown("---")
st.markdown("<p style='text-align: center; color: #64748b;'>© 2025 Sistema de Análisis UNAL | Versión Optimizada 2.3</p>", unsafe_allow_html=True)

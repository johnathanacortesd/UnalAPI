# ==============================================================================
# ANÁLISIS DE TONO Y TEMA PARA UNIVERSIDAD NACIONAL - APP STREAMLIT (OPTIMIZADA)
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
import asyncio
from concurrent.futures import ThreadPoolExecutor

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

# ==============================================================================
# ESTILOS CSS PERSONALIZADOS
# ==============================================================================
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #64748B;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0.5rem 0;
    }
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
    }
    .success-box {
        background-color: #D1FAE5;
        border-left: 4px solid #10B981;
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #DBEAFE;
        border-left: 4px solid #3B82F6;
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #FEF3C7;
        border-left: 4px solid #F59E0B;
        padding: 1rem;
        border-radius: 4px;
        margin: 1rem 0;
    }
    .stButton>button {
        width: 100%;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        padding: 0.75rem 1.5rem;
        font-size: 1.1rem;
        font-weight: 600;
        border-radius: 8px;
        transition: all 0.3s ease;
    }
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 16px rgba(0,0,0,0.2);
    }
    .file-upload-section {
        background-color: #F8FAFC;
        padding: 1.5rem;
        border-radius: 8px;
        margin-bottom: 1rem;
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

    st.markdown('<p class="main-header">🔐 Acceso Protegido</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Por favor, introduce la contraseña para acceder al sistema</p>', unsafe_allow_html=True)
    
    with st.form("password_form"):
        password = st.text_input("Contraseña", type="password", placeholder="Ingresa tu contraseña")
        submitted = st.form_submit_button("🚀 Ingresar", use_container_width=True)

        if submitted:
            correct_password = st.secrets.get("APP_PASSWORD")
            if not correct_password:
                st.error("❌ Error de configuración: No se ha establecido una contraseña.")
                return False
            
            if password == correct_password:
                st.session_state["password_correct"] = True
                st.rerun()
            else:
                st.error("❌ Contraseña incorrecta. Por favor, inténtalo de nuevo.")
    return False

# ==============================================================================
# FUNCIONES AUXILIARES (OPTIMIZADAS)
# ==============================================================================

def norm_key(text: Any) -> str:
    """Normaliza texto para usar como clave."""
    if text is None: 
        return ""
    return re.sub(r"[^a-z0-9]+", "", unidecode(str(text).strip().lower()))

def corregir_texto(text: Any) -> str:
    """Limpia y corrige formato de texto."""
    if not isinstance(text, str): 
        return ""
    text = re.sub(r'(<br\s*/?>|\[\.\.\.\])+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    match = re.search(r"[A-ZÁÉÍÓÚÑ]", text)
    if match: 
        text = text[match.start():]
    return text

def clean_title_for_output(title: Any) -> str:
    """Limpia título para salida."""
    if not isinstance(title, str): 
        return str(title if title is not None else "")
    return re.sub(r"\s*\|\s*[\w\s]+$", "", title).strip()

def normalizar_tipo_medio(tipo_raw: str) -> str:
    """Normaliza tipo de medio."""
    if not isinstance(tipo_raw, str): 
        return str(tipo_raw)
    t = unidecode(str(tipo_raw).strip().lower())
    mapping = {
        "fm": "Radio", "am": "Radio", "radio": "Radio", 
        "aire": "Televisión", "cable": "Televisión", "tv": "Televisión",
        "television": "Televisión", "televisión": "Televisión", 
        "senal abierta": "Televisión", "señal abierta": "Televisión",
        "diario": "Prensa", "prensa": "Prensa", 
        "revista": "Revista", "revistas": "Revista", 
        "online": "Internet", "internet": "Internet", 
        "digital": "Internet", "web": "Internet"
    }
    return mapping.get(t, str(tipo_raw).strip().title() if str(tipo_raw).strip() else "Otro")

def extract_link(cell):
    """Extrae enlaces de celdas Excel."""
    if hasattr(cell, "hyperlink") and cell.hyperlink and cell.hyperlink.target:
        return {"value": cell.value or "Link", "url": cell.hyperlink.target}
    if isinstance(cell.value, str) and "=HYPERLINK" in cell.value:
        match = re.search(r'=HYPERLINK\("([^"]+)"', cell.value)
        if match: 
            return {"value": "Link", "url": match.group(1)}
    return {"value": cell.value, "url": None}

def run_base_logic(sheet, progress_hook):
    """Procesa la lógica base del archivo Excel."""
    headers = [c.value for c in sheet[1] if c.value]
    norm_keys = [norm_key(h) for h in headers]
    key_map = {nk: nk for nk in norm_keys}
    key_map.update({
        "titulo": norm_key("Titulo"), 
        "resumen": norm_key("Resumen - Aclaracion"), 
        "menciones": norm_key("Menciones - Empresa"),
        "medio": norm_key("Medio"), 
        "tonoai": norm_key("Tono AI"), 
        "tema": norm_key("Tema"), 
        "idnoticia": norm_key("ID Noticia"),
        "idduplicada": norm_key("ID duplicada"), 
        "tipodemedio": norm_key("Tipo de Medio"), 
        "link_nota": norm_key("Link Nota"),
        "link_streaming": norm_key("Link (Streaming - Imagen)"), 
        "region": norm_key("Region"), 
        "hora": norm_key("Hora")
    })
    
    rows = [{norm_keys[i]: cell for i, cell in enumerate(row) if i < len(norm_keys)}
            for row in sheet.iter_rows(min_row=2) if not all(c.value is None for c in row)]
    
    split_rows = []
    for r_cells in rows:
        base = {k: extract_link(v) if k in [key_map["link_nota"], key_map["link_streaming"]] else v.value 
                for k, v in r_cells.items()}
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
    """Detecta duplicados usando múltiples criterios."""
    processed_rows = deepcopy(rows)
    seen_text_key, seen_online_url, seen_broadcast = {}, {}, {}
    
    id_key = key_map.get("idnoticia", "idnoticia")
    id_duplicada_key = key_map.get("idduplicada", "idduplicada")
    titulo_key = key_map.get("titulo", "titulo")
    resumen_key = key_map.get("resumen", "resumen")
    mencion_key = key_map.get("menciones", "menciones")
    medio_key = key_map.get("medio", "medio")
    tipo_medio_key = key_map.get("tipodemedio")
    link_nota_key = key_map.get("link_nota")
    hora_key = key_map.get("hora")
    
    total_rows = len(processed_rows)
    
    for i, row in enumerate(processed_rows):
        if i % 100 == 0:  # Actualizar cada 100 filas
            progress_hook(i, total_rows, "Detectando duplicados...")
        
        mencion_norm = norm_key(row.get(mencion_key))
        medio_norm = norm_key(row.get(medio_key))
        texto_titulo = corregir_texto(row.get(titulo_key, ''))
        texto_resumen = corregir_texto(row.get(resumen_key, ''))
        texto_base = texto_titulo if texto_titulo else texto_resumen
        
        if texto_base:
            clave_texto_norm = norm_key(" ".join(texto_base.split()[:3]))
            key = (clave_texto_norm, mencion_norm, medio_norm)
            
            if clave_texto_norm and key in seen_text_key:
                row["is_duplicate"] = True
                row[id_duplicada_key] = processed_rows[seen_text_key[key]].get(id_key, "")
                continue
            else: 
                seen_text_key[key] = i
        
        tipo_medio = normalizar_tipo_medio(str(row.get(tipo_medio_key)))
        
        if tipo_medio == "Internet":
            url = (row.get(link_nota_key, {}) or {}).get("url")
            if url and mencion_norm:
                key_url = (url, mencion_norm)
                if key_url in seen_online_url:
                    row["is_duplicate"] = True
                    row[id_duplicada_key] = processed_rows[seen_online_url[key_url]].get(id_key, "")
                else: 
                    seen_online_url[key_url] = i
        
        elif tipo_medio in ["Radio", "Televisión"]:
            hora = str(row.get(hora_key, "")).strip()
            if mencion_norm and medio_norm and hora:
                key_broadcast = (mencion_norm, medio_norm, hora)
                if key_broadcast in seen_broadcast:
                    row["is_duplicate"] = True
                    row[id_duplicada_key] = processed_rows[seen_broadcast[key_broadcast]].get(id_key, "")
                else: 
                    seen_broadcast[key_broadcast] = i
    
    return processed_rows

def process_mappings_and_links(all_processed_rows, key_map, region_file, internet_file):
    """Procesa mapeos de región e internet."""
    df_region = pd.read_excel(region_file)
    region_map = {str(k).lower().strip(): v 
                  for k, v in pd.Series(df_region.iloc[:, 1].values, 
                                       index=df_region.iloc[:, 0]).to_dict().items()}
    
    df_internet = pd.read_excel(internet_file)
    internet_map = {str(k).lower().strip(): v 
                    for k, v in pd.Series(df_internet.iloc[:, 1].values, 
                                         index=df_internet.iloc[:, 0]).to_dict().items()}
    
    for row in all_processed_rows:
        original_medio_key = str(row.get(key_map.get("medio"), "")).lower().strip()
        row[key_map.get("region")] = region_map.get(original_medio_key, "N/A")
        
        if original_medio_key in internet_map:
            row[key_map.get("medio")] = internet_map[original_medio_key]
            row[key_map.get("tipodemedio")] = "Internet"
    
    return all_processed_rows

def process_link_logic(all_rows, key_map):
    """Procesa lógica de enlaces según tipo de medio."""
    ln_key = key_map.get("link_nota")
    ls_key = key_map.get("link_streaming")
    
    for row in all_rows:
        tipo = normalizar_tipo_medio(row.get(key_map.get("tipodemedio"), ""))
        ln = row.get(ln_key) or {}
        ls = row.get(ls_key) or {}
        has_url = lambda x: isinstance(x, dict) and bool(x.get("url"))
        
        if tipo in ["Radio", "Televisión"]: 
            row[ls_key] = None
        elif tipo == "Internet": 
            row[ln_key], row[ls_key] = ls, ln
        elif tipo in ["Prensa", "Revista"]:
            if not has_url(ln) and has_url(ls): 
                row[ln_key] = ls
            row[ls_key] = None
    
    return all_rows

def process_sov_mapping_final(all_rows: List[Dict], key_map: Dict[str, str], sov_file):
    """Procesa mapeo SOV final."""
    df_sov = pd.read_excel(sov_file)
    cols_by_norm = {norm_key(c): c for c in df_sov.columns}
    menc_col = cols_by_norm.get(norm_key("Menciones - Empresa"))
    name_col = cols_by_norm.get(norm_key("Nombre"))
    
    if not menc_col or not name_col: 
        return all_rows
    
    sov_map = {str(r.get(menc_col, "")).strip().lower(): str(r.get(name_col)).strip() 
               for _, r in df_sov.iterrows() 
               if str(r.get(menc_col, "")).strip() and str(r.get(name_col, "")).strip()}
    
    for r in all_rows:
        mk = str(r.get(key_map.get("menciones"), "")).strip().lower()
        if mk in sov_map: 
            r[key_map.get("menciones")] = sov_map[mk]
    
    return all_rows

def _append_rows_to_sheet(sheet, rows_data, key_map, include_ai_columns):
    """Agrega filas a hoja Excel."""
    base_order = [
        "ID Noticia", "Fecha", "Hora", "Medio", "Tipo de Medio", "Seccion - Programa", 
        "Region", "Titulo", "Autor - Conductor", "Nro. Pagina", "Dimension", 
        "Duracion - Nro. Caracteres", "CPE", "Tier", "Audiencia", "Tono", 
        "Resumen - Aclaracion", "Link Nota", "Link (Streaming - Imagen)", 
        "Menciones - Empresa", "ID duplicada"
    ]
    ai_order = ["Tono AI", "Tema"]
    final_order = base_order[:16] + ai_order + base_order[16:] if include_ai_columns else base_order
    
    sheet.append(final_order)
    
    for row_data in rows_data:
        row_data[key_map.get("titulo")] = clean_title_for_output(row_data.get(key_map.get("titulo")))
        row_data[key_map.get("resumen")] = corregir_texto(str(row_data.get(key_map.get("resumen"), ""))).replace("_x000D_", "")
        
        row_to_append = []
        links_to_add = {}
        
        for col_idx, header in enumerate(final_order, 1):
            val = row_data.get(norm_key(header))
            cell_value = None
            
            if isinstance(val, dict) and val.get("url"):
                cell_value = "Link"
                url = val.get("url")
                links_to_add[col_idx] = url
            elif val is not None: 
                cell_value = str(val)
            
            row_to_append.append(cell_value)
        
        sheet.append(row_to_append)
        
        for col_idx, url in links_to_add.items():
            cell = sheet.cell(row=sheet.max_row, column=col_idx)
            cell.hyperlink = url
            cell.style = "Hyperlink"

def generate_excel_output(all_processed_rows, key_map):
    """Genera archivo Excel de salida."""
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

def agrupar_noticias_similares(rows: List[Dict], key_map: Dict[str, str]) -> Dict[str, List[int]]:
    """Agrupa noticias similares para procesamiento en lote."""
    grupos = {}
    titulo_key = key_map.get("titulo", "titulo")
    resumen_key = key_map.get("resumen", "resumen")
    
    for idx, row in enumerate(rows):
        titulo = corregir_texto(row.get(titulo_key, ''))
        resumen = corregir_texto(row.get(resumen_key, ''))
        clave_grupo = " ".join(titulo.strip().split()[:4]) if titulo else " ".join(resumen.strip().split()[:4])
        clave_grupo_norm = norm_key(clave_grupo)
        
        if clave_grupo_norm:
            if clave_grupo_norm not in grupos: 
                grupos[clave_grupo_norm] = []
            grupos[clave_grupo_norm].append(idx)
    
    return grupos

# ==============================================================================
# CLASE DE SEGUIMIENTO DE COSTOS
# ==============================================================================

class CostTracker:
    """Rastrea y gestiona costos de API."""
    
    def __init__(self, limit_usd: float, input_cost_per_1m: float, output_cost_per_1m: float):
        self.limit_usd = limit_usd
        self.total_cost = 0.0
        self.input_cost_per_token = input_cost_per_1m / 1e6
        self.output_cost_per_token = output_cost_per_1m / 1e6
        self.total_input_tokens = 0
        self.total_output_tokens = 0
    
    def add_cost(self, input_tokens: int, output_tokens: int):
        """Agrega costo de una llamada a la API."""
        cost = (input_tokens * self.input_cost_per_token) + (output_tokens * self.output_cost_per_token)
        self.total_cost += cost
        self.total_input_tokens += input_tokens
        self.total_output_tokens += output_tokens
    
    def is_limit_exceeded(self) -> bool:
        """Verifica si se excedió el límite de costo."""
        return self.total_cost >= self.limit_usd
    
    def get_summary(self) -> Dict[str, Any]:
        """Retorna resumen de costos."""
        return {
            "limit": self.limit_usd,
            "total_cost": self.total_cost,
            "input_tokens": self.total_input_tokens,
            "output_tokens": self.total_output_tokens,
            "percentage": (self.total_cost / self.limit_usd * 100) if self.limit_usd > 0 else 0
        }

# ==============================================================================
# FUNCIÓN DE ANÁLISIS CON OPENAI (OPTIMIZADA CON PROCESAMIENTO PARALELO)
# ==============================================================================

def analizar_con_openai(textos_agrupados: List[Tuple[str, List[int]]], 
                        cost_tracker: CostTracker, 
                        client: OpenAI, 
                        progress_hook):
    """Analiza noticias con OpenAI usando procesamiento por lotes."""
    resultados = {}
    
    tools = [{
        "type": "function",
        "function": {
            "name": "clasificar_noticia_unal",
            "description": "Clasifica el tono y el tema de una noticia sobre la Universidad Nacional.",
            "parameters": {
                "type": "object",
                "properties": {
                    "tono": {
                        "type": "string",
                        "description": "El tono de la noticia: Positivo, Negativo o Neutro.",
                        "enum": ["Positivo", "Negativo", "Neutro"]
                    },
                    "tema": {
                        "type": "string",
                        "description": "Tema específico de 4 a 6 palabras que resume el hecho principal."
                    }
                },
                "required": ["tono", "tema"]
            }
        }
    }]
    
    system_prompt = """Eres un analista de medios experto especializado en la Universidad Nacional de Colombia. 

**REGLAS DE TONO:**
- **NEGATIVO:** Crisis, críticas, efectos adversos, controversias, violencia en campus
- **POSITIVO:** Rankings, innovación, programas exitosos, gestiones a favor, reconocimientos
- **NEUTRO:** Menciones generales, informativas, sin valoración

**REGLAS PARA EL TEMA:**
1. Describe el evento o hecho principal específico
2. NO incluyas "Universidad Nacional" o "UNAL"
3. Usa entre 4 y 6 palabras
4. Sé específico, no genérico

Ejemplo INCORRECTO: "Mención en contexto de violencia"
Ejemplo CORRECTO: "Disturbios en campus por protestas estudiantiles"
"""
    
    total_grupos = len(textos_agrupados)
    
    for i, (texto_representativo, indices_grupo) in enumerate(textos_agrupados):
        if i % 10 == 0:  # Actualizar cada 10 grupos
            progress_hook(i, total_grupos, f"Analizando grupo {i+1}/{total_grupos}...")
        
        if cost_tracker.is_limit_exceeded():
            st.warning(f"⚠️ Límite de costo alcanzado (${cost_tracker.limit_usd:.2f})")
            break
        
        try:
            response = client.chat.completions.create(
                model="gpt-4.1-nano-2025-04-14",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Analiza: \"{texto_representativo}\""}
                ],
                tools=tools,
                tool_choice={"type": "function", "function": {"name": "clasificar_noticia_unal"}},
                temperature=0.0,
                max_tokens=150  # Reducido de 250 a 150 para mayor velocidad
            )
            
            if response.usage:
                cost_tracker.add_cost(response.usage.prompt_tokens, response.usage.completion_tokens)
            
            resultado_json = json.loads(response.choices[0].message.tool_calls[0].function.arguments)
            tono = resultado_json["tono"]
            tema = resultado_json["tema"]
            
            for idx in indices_grupo:
                resultados[idx] = {"tono": tono, "tema": tema}
        
        except Exception as e:
            st.error(f"❌ Error en grupo {i + 1}: {str(e)[:100]}")
            for idx in indices_grupo:
                resultados[idx] = {"tono": "Error", "tema": "Error en procesamiento"}
    
    return resultados

# ==============================================================================
# PROCESAMIENTO POR LOTES (OPTIMIZADO)
# ==============================================================================

def procesar_por_lotes(target_rows: List[Dict], 
                       key_map: Dict[str, str], 
                       batch_size: int, 
                       cost_tracker: CostTracker, 
                       client: OpenAI, 
                       status_placeholder, 
                       progress_bar):
    """Procesa noticias en lotes para análisis IA."""
    total_rows = len(target_rows)
    num_batches = (total_rows + batch_size - 1) // batch_size
    
    titulo_key = key_map.get("titulo")
    resumen_key = key_map.get("resumen")
    tono_key = key_map.get("tonoai")
    tema_key = key_map.get("tema")
    
    all_resultados = []
    
    for batch_num in range(num_batches):
        if cost_tracker.is_limit_exceeded():
            break
        
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, total_rows)
        batch_rows = target_rows[start_idx:end_idx]
        
        status_placeholder.text(f"📦 Lote {batch_num + 1}/{num_batches} (Noticias {start_idx + 1}-{end_idx})")
        
        # Agrupar noticias similares
        grupos = agrupar_noticias_similares(batch_rows, key_map)
        textos_agrupados = []
        
        for _, indices_locales in grupos.items():
            idx_repr = indices_locales[0]
            titulo = corregir_texto(batch_rows[idx_repr].get(titulo_key, ''))
            resumen = corregir_texto(batch_rows[idx_repr].get(resumen_key, ''))
            texto_completo = f"{titulo}. {resumen}".strip()[:2500]  # Reducido de 3000 a 2500
            
            if texto_completo:
                textos_agrupados.append((texto_completo, indices_locales))
        
        def progress_hook_ia(current, total, text):
            base_progress = (batch_num / num_batches)
            lote_progress = (current / total) * (1 / num_batches) if total > 0 else 0
            progress_bar.progress(base_progress + lote_progress, 
                                text=f"Lote {batch_num+1}/{num_batches}: {text}")
        
        # Analizar con IA
        resultados_batch = analizar_con_openai(textos_agrupados, cost_tracker, client, progress_hook_ia)
        
        # Aplicar resultados
        for idx_local, resultado in resultados_batch.items():
            batch_rows[idx_local][tono_key] = resultado["tono"]
            batch_rows[idx_local][tema_key] = resultado["tema"]
        
        all_resultados.extend(batch_rows)
    
    return all_resultados

# ==============================================================================
# INTERFAZ PRINCIPAL
# ==============================================================================

def main():
    """Función principal de la aplicación."""
    
    # Verificar autenticación
    if not check_password():
        st.stop()
    
    # Verificar API Key
    api_key = st.secrets.get("OPENAI_API_KEY")
    if not api_key:
        st.error("❌ Error: API Key de OpenAI no configurada en secrets")
        st.stop()
    
    # Header
    st.markdown('<p class="main-header">🎓 Análisis de Tono y Tema - Universidad Nacional</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Sistema automatizado de análisis de menciones en medios con IA | Desarrollado por Johnathan Cortés ©️</p>', unsafe_allow_html=True)
    
    # Inicializar session state
    if 'analysis_done' not in st.session_state:
        st.session_state.analysis_done = False
    if 'result_buffer' not in st.session_state:
        st.session_state.result_buffer = None
    if 'final_summary' not in st.session_state:
        st.session_state.final_summary = {}
    if 'analysis_stats' not in st.session_state:
        st.session_state.analysis_stats = {}
    
    # ==============================================================================
    # SIDEBAR - CONFIGURACIÓN
    # ==============================================================================
    
    with st.sidebar:
        st.markdown("### 📁 Carga de Archivos")
        st.markdown('<div class="file-upload-section">', unsafe_allow_html=True)
        
        dossier_file = st.file_uploader(
            "Dossier Principal", 
            type="xlsx",
            help="Archivo principal con las noticias a analizar"
        )
        
        region_file = st.file_uploader(
            "Mapeo de Región", 
            type="xlsx",
            help="Archivo de mapeo de regiones por medio"
        )
        
        internet_file = st.file_uploader(
            "Mapeo de Internet", 
            type="xlsx",
            help="Archivo de mapeo de medios digitales"
        )
        
        sov_file = st.file_uploader(
            "Mapeo SOV", 
            type="xlsx",
            help="Archivo de mapeo de menciones"
        )
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown("---")
        st.markdown("### ⚙️ Configuración del Análisis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            cost_limit_usd = st.number_input(
                "💰 Límite (USD)",
                min_value=0.10,
                max_value=10.0,
                value=1.00,
                step=0.10,
                help="Costo máximo permitido para el análisis con IA"
            )
        
        with col2:
            batch_size = st.selectbox(
                "📦 Tamaño Lote",
                options=[200, 300, 400, 500, 600],
                index=2,  # 400 por defecto
                help="Noticias procesadas por lote (menor = más rápido pero más llamadas)"
            )
        
        st.markdown("---")
        
        # Información adicional
        with st.expander("ℹ️ Información del Sistema"):
            st.markdown("""
            **Optimizaciones Aplicadas:**
            - ✅ Procesamiento por lotes optimizado
            - ✅ Detección de duplicados mejorada
            - ✅ Agrupación inteligente de noticias
            - ✅ Límites de tokens reducidos (150)
            - ✅ Textos recortados a 2500 caracteres
            - ✅ Actualización de progreso cada 10 grupos
            
            **Recomendaciones:**
            - Lotes de 400 noticias = velocidad óptima
            - Lotes de 200 noticias = máxima velocidad
            - Lotes de 600 noticias = menor costo
            """)
        
        st.markdown("---")
        
        # Botón de análisis
        all_files_uploaded = all([dossier_file, region_file, internet_file, sov_file])
        
        if not all_files_uploaded:
            st.warning("⚠️ Carga todos los archivos requeridos")
        
        analyze_button = st.button(
            "🚀 Iniciar Análisis Completo",
            type="primary",
            disabled=not all_files_uploaded,
            use_container_width=True
        )
    
    # ==============================================================================
    # ÁREA PRINCIPAL - RESULTADOS Y PROCESAMIENTO
    # ==============================================================================
    
    if analyze_button:
        # Resetear estados
        st.session_state.analysis_done = False
        st.session_state.result_buffer = None
        st.session_state.final_summary = {}
        st.session_state.analysis_stats = {}
        
        # Inicializar cliente OpenAI
        client = OpenAI(api_key=api_key)
        
        # Marcas objetivo
        TARGET_BRANDS = [
            "U. Nacional de Colombia",
            "Universidad Nacional de Colombia",
            "Universidad Nacional de Colombia - General"
        ]
        
        # Contenedor de progreso
        progress_container = st.container()
        
        with progress_container:
            # Barra de progreso principal
            main_progress = st.progress(0, text="Iniciando análisis...")
            status_text = st.empty()
            
            def main_progress_hook(current, total, text):
                if total > 0:
                    progress = current / total
                    main_progress.progress(progress, text=text)
            
            try:
                # ==============================================================================
                # FASE 1: PREPARACIÓN DE DATOS
                # ==============================================================================
                
                with st.spinner("⏳ Preparando datos..."):
                    status_text.markdown('<div class="info-box">📋 <strong>Fase 1/3:</strong> Cargando y limpiando datos...</div>', unsafe_allow_html=True)
                    
                    start_time = time.time()
                    
                    # Cargar y procesar archivo principal
                    wb = load_workbook(dossier_file, data_only=True)
                    all_processed_rows, key_map = run_base_logic(wb.active, main_progress_hook)
                    
                    # Aplicar mapeos
                    all_processed_rows = process_mappings_and_links(
                        all_processed_rows, key_map, region_file, internet_file
                    )
                    all_processed_rows = process_link_logic(all_processed_rows, key_map)
                    
                    # Marcar noticias objetivo
                    for row in all_processed_rows:
                        row["__is_target_brand"] = (
                            row.get(key_map.get("menciones")) in TARGET_BRANDS
                        )
                    
                    # Estadísticas iniciales
                    total_noticias = len(all_processed_rows)
                    noticias_unal = sum(1 for r in all_processed_rows if r.get('__is_target_brand'))
                    duplicadas = sum(1 for r in all_processed_rows if r.get('is_duplicate'))
                    noticias_analizar = sum(1 for r in all_processed_rows 
                                           if r.get('__is_target_brand') and not r.get('is_duplicate'))
                    
                    phase1_time = time.time() - start_time
                    
                    # Mostrar estadísticas fase 1
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.markdown(f"""
                        <div class="metric-card" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);">
                            <div class="metric-label">Total Noticias</div>
                            <div class="metric-value">{total_noticias:,}</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col2:
                        st.markdown(f"""
                        <div class="metric-card" style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);">
                            <div class="metric-label">Noticias UNAL</div>
                            <div class="metric-value">{noticias_unal:,}</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col3:
                        st.markdown(f"""
                        <div class="metric-card" style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);">
                            <div class="metric-label">Duplicadas</div>
                            <div class="metric-value">{duplicadas:,}</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col4:
                        st.markdown(f"""
                        <div class="metric-card" style="background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);">
                            <div class="metric-label">A Analizar</div>
                            <div class="metric-value">{noticias_analizar:,}</div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    main_progress.progress(0.33, text="✅ Fase 1 completada")
                    time.sleep(0.5)
                
                # ==============================================================================
                # FASE 2: ANÁLISIS CON IA
                # ==============================================================================
                
                status_text.markdown('<div class="info-box">🤖 <strong>Fase 2/3:</strong> Analizando con Inteligencia Artificial...</div>', unsafe_allow_html=True)
                
                cost_tracker = CostTracker(cost_limit_usd, 0.10, 0.40)
                target_rows = [
                    row for row in all_processed_rows 
                    if row.get("__is_target_brand") and not row.get("is_duplicate")
                ]
                
                phase2_start = time.time()
                status_placeholder = st.empty()
                
                if target_rows:
                    target_rows_procesados = procesar_por_lotes(
                        target_rows, 
                        key_map, 
                        batch_size, 
                        cost_tracker, 
                        client, 
                        status_placeholder, 
                        main_progress
                    )
                    
                    # Actualizar filas originales
                    update_map = {row['original_index']: row for row in target_rows_procesados}
                    for row in all_processed_rows:
                        if row['original_index'] in update_map:
                            row[key_map.get("tonoai")] = update_map[row['original_index']].get(key_map.get("tonoai"))
                            row[key_map.get("tema")] = update_map[row['original_index']].get(key_map.get("tema"))
                    
                    st.session_state.final_summary = cost_tracker.get_summary()
                    phase2_time = time.time() - phase2_start
                else:
                    status_text.markdown('<div class="warning-box">⚠️ No hay noticias nuevas de la UNAL para analizar</div>', unsafe_allow_html=True)
                    phase2_time = 0
                
                main_progress.progress(0.66, text="✅ Fase 2 completada")
                time.sleep(0.5)
                
                # ==============================================================================
                # FASE 3: GENERACIÓN DE INFORME
                # ==============================================================================
                
                status_text.markdown('<div class="info-box">📄 <strong>Fase 3/3:</strong> Generando informe final...</div>', unsafe_allow_html=True)
                
                phase3_start = time.time()
                
                final_rows = process_sov_mapping_final(all_processed_rows, key_map, sov_file)
                st.session_state.result_buffer = generate_excel_output(final_rows, key_map)
                
                phase3_time = time.time() - phase3_start
                total_time = time.time() - start_time
                
                main_progress.progress(1.0, text="✅ ¡Análisis completado!")
                
                # Guardar estadísticas
                st.session_state.analysis_stats = {
                    "total_noticias": total_noticias,
                    "noticias_unal": noticias_unal,
                    "duplicadas": duplicadas,
                    "analizadas": noticias_analizar,
                    "phase1_time": phase1_time,
                    "phase2_time": phase2_time,
                    "phase3_time": phase3_time,
                    "total_time": total_time
                }
                
                st.session_state.analysis_done = True
                
                # Mensaje de éxito
                status_text.markdown('<div class="success-box">🎉 <strong>¡Análisis completado exitosamente!</strong> Descarga tu informe abajo.</div>', unsafe_allow_html=True)
                
                # Efecto de confeti (opcional)
                st.balloons()
            
            except Exception as e:
                st.error(f"❌ Error durante el procesamiento: {str(e)}")
                st.exception(e)
                st.session_state.analysis_done = False
    
    # ==============================================================================
    # MOSTRAR RESULTADOS
    # ==============================================================================
    
    if st.session_state.analysis_done and st.session_state.result_buffer:
        st.markdown("---")
        st.markdown("## 📊 Resultados del Análisis")
        
        # Crear tabs para organizar la información
        tab1, tab2, tab3 = st.tabs(["💰 Costos y Tokens", "📈 Estadísticas", "⏱️ Tiempos"])
        
        with tab1:
            summary = st.session_state.final_summary
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Costo Total</div>
                    <div class="metric-value">${summary.get('total_cost', 0):.4f}</div>
                    <div class="metric-label">Límite: ${summary.get('limit', 0):.2f}</div>
                </div>
                """, unsafe_allow_html=True)
                
                # Barra de progreso de costo
                percentage = summary.get('percentage', 0)
                color = "#10B981" if percentage < 80 else "#F59E0B" if percentage < 95 else "#EF4444"
                st.progress(min(percentage / 100, 1.0), text=f"Uso del presupuesto: {percentage:.1f}%")
            
            with col2:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-label">Tokens Procesados</div>
                    <div class="metric-value">{summary.get('input_tokens', 0) + summary.get('output_tokens', 0):,}</div>
                    <div class="metric-label">Entrada: {summary.get('input_tokens', 0):,} | Salida: {summary.get('output_tokens', 0):,}</div>
                </div>
                """, unsafe_allow_html=True)
                
                # Desglose de tokens
                if summary.get('input_tokens', 0) + summary.get('output_tokens', 0) > 0:
                    input_pct = summary.get('input_tokens', 0) / (summary.get('input_tokens', 0) + summary.get('output_tokens', 0)) * 100
                    st.info(f"📥 Entrada: {input_pct:.1f}% | 📤 Salida: {100-input_pct:.1f}%")
        
        with tab2:
            stats = st.session_state.analysis_stats
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total de Noticias", f"{stats.get('total_noticias', 0):,}")
                st.metric("Noticias UNAL", f"{stats.get('noticias_unal', 0):,}")
            
            with col2:
                st.metric("Duplicadas", f"{stats.get('duplicadas', 0):,}")
                st.metric("Analizadas con IA", f"{stats.get('analizadas', 0):,}")
            
            with col3:
                if stats.get('analizadas', 0) > 0:
                    efectividad = (stats.get('analizadas', 0) / stats.get('noticias_unal', 1)) * 100
                    st.metric("Efectividad", f"{efectividad:.1f}%")
                    st.metric("Tasa de Duplicados", f"{(stats.get('duplicadas', 0) / stats.get('total_noticias', 1) * 100):.1f}%")
        
        with tab3:
            stats = st.session_state.analysis_stats
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### ⏱️ Tiempo por Fase")
                st.metric("Fase 1: Preparación", f"{stats.get('phase1_time', 0):.1f}s")
                st.metric("Fase 2: Análisis IA", f"{stats.get('phase2_time', 0):.1f}s")
                st.metric("Fase 3: Generación", f"{stats.get('phase3_time', 0):.1f}s")
            
            with col2:
                st.markdown("### 🚀 Rendimiento")
                total = stats.get('total_time', 1)
                st.metric("Tiempo Total", f"{total:.1f}s")
                
                if stats.get('analizadas', 0) > 0 and stats.get('phase2_time', 0) > 0:
                    velocidad = stats.get('analizadas', 0) / stats.get('phase2_time', 1)
                    st.metric("Velocidad IA", f"{velocidad:.1f} noticias/seg")
                
                st.progress(1.0, text="Proceso completado")
        
        st.markdown("---")
        
        # Botón de descarga prominente
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col2:
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M')
            st.download_button(
                label="📥 Descargar Informe Completo",
                data=st.session_state.result_buffer,
                file_name=f"Informe_UNAL_{timestamp}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                type="primary"
            )
    
    elif not st.session_state.analysis_done:
        # Instrucciones iniciales
        st.markdown("---")
        st.markdown("## 🚀 Comienza tu Análisis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            <div class="info-box">
                <h3>📋 Paso 1: Carga de Archivos</h3>
                <p>Sube los 4 archivos requeridos en la barra lateral:</p>
                <ul>
                    <li>📂 Dossier Principal (.xlsx)</li>
                    <li>🌍 Mapeo de Región (.xlsx)</li>
                    <li>🌐 Mapeo de Internet (.xlsx)</li>
                    <li>📊 Mapeo SOV (.xlsx)</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="info-box">
                <h3>⚙️ Paso 2: Configuración</h3>
                <p>Ajusta los parámetros del análisis:</p>
                <ul>
                    <li>💰 <strong>Límite de Costo:</strong> Presupuesto máximo en USD</li>
                    <li>📦 <strong>Tamaño de Lote:</strong> Noticias por tanda
                        <ul>
                            <li>200: Máxima velocidad</li>
                            <li>400: Velocidad óptima (recomendado)</li>
                            <li>600: Menor costo</li>
                        </ul>
                    </li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="info-box" style="text-align: center; margin-top: 2rem;">
            <h3>✨ Características del Sistema</h3>
            <p><strong>Velocidad Optimizada:</strong> Procesamiento hasta 2x más rápido con lotes de 400 noticias</p>
            <p><strong>Control de Costos:</strong> Monitoreo en tiempo real y límites configurables</p>
            <p><strong>Detección Inteligente:</strong> Eliminación automática de duplicados y agrupación de noticias similares</p>
            <p><strong>Análisis de IA:</strong> Clasificación de tono y tema con gpt-4.1-nano-2025-04-14</p>
        </div>
        """, unsafe_allow_html=True)

# ==============================================================================
# EJECUTAR APLICACIÓN
# ==============================================================================

if __name__ == "__main__":
    main()

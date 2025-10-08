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
SIMILARITY_THRESHOLD_TITULOS = 0.95 # Elevado para ser más estricto con títulos casi idénticos
MAX_TOKENS_PROMPT_TXT = 4000
WINDOW = 80
NUM_TEMAS_PRINCIPALES = 30 # Número máximo de temas principales a generar

# Listas para limpieza de temas (pueden ser expandidas si es necesario)
CIUDADES_COLOMBIA = { "bogotá", "bogota", "medellín", "medellin", "cali", "barranquilla", "cartagena", "cúcuta", "cucuta", "bucaramanga", "pereira", "manizales", "armenia", "ibagué", "ibague", "villavicencio", "montería", "monteria", "neiva", "pasto", "valledupar", "popayán", "popayan", "tunja", "florencia", "sincelejo", "riohacha", "yopal", "santa marta", "santamarta", "quibdó", "quibdo", "leticia", "mocoa", "mitú", "mitu", "puerto carreño", "inírida", "inirida", "san josé del guaviare", "antioquia", "atlántico", "atlantico", "bolívar", "bolivar", "boyacá", "boyaca", "caldas", "caquetá", "caqueta", "casanare", "cauca", "cesar", "chocó", "choco", "córdoba", "cordoba", "cundinamarca", "guainía", "guainia", "guaviare", "huila", "la guajira", "magdalena", "meta", "nariño", "narino", "norte de santander", "putumayo", "quindío", "quindio", "risaralda", "san andrés", "san andres", "santander", "sucre", "tolima", "valle del cauca", "vaupés", "vaupes", "vichada"}
GENTILICIOS_COLOMBIA = {"bogotano", "bogotanos", "bogotana", "bogotanas", "capitalino", "capitalinos", "capitalina", "capitalinas", "antioqueño", "antioqueños", "antioqueña", "antioqueñas", "paisa", "paisas", "medellense", "medellenses", "caleño", "caleños", "caleña", "caleñas", "valluno", "vallunos", "valluna", "vallunas", "vallecaucano", "vallecaucanos", "barranquillero", "barranquilleros", "cartagenero", "cartageneros", "costeño", "costeños", "costeña", "costeñas", "cucuteño", "cucuteños", "bumangués", "santandereano", "santandereanos", "boyacense", "boyacenses", "tolimense", "tolimenses", "huilense", "huilenses", "nariñense", "nariñenses", "pastuso", "pastusas", "cordobés", "cordobeses", "cauca", "caucano", "caucanos", "chocoano", "chocoanos", "casanareño", "casanareños", "caqueteño", "caqueteños", "guajiro", "guajiros", "llanero", "llaneros", "amazonense", "amazonenses", "colombiano", "colombianos", "colombiana", "colombianas"}

# ======================================
# Lexicos y patrones para analisis de tono
# ======================================
STOPWORDS_ES = set(""" a ante bajo cabe con contra de desde durante en entre hacia hasta mediante para por segun sin so sobre tras y o u e la el los las un una unos unas lo al del se su sus le les mi mis tu tus nuestro nuestros vuestra vuestras este esta estos estas ese esa esos esas aquel aquella aquellos aquellas que cual cuales quien quienes cuyo cuya cuyos cuyas como cuando donde cual es son fue fueron era eran sera seran seria serian he ha han habia habian hay hubo habra habria estoy esta estan estaba estaban estamos estan estar estare estaria estuvieron estarian estuvo asi ya mas menos tan tanto cada """.split())
POS_VARIANTS = [ r"lanz(a(r|ra|ria|o|on|an|ando)?|amiento)s?", r"prepar(a|ando)", r"nuev[oa]\s+(servicio|tienda|plataforma|app|aplicacion|funcion|canal|portal|producto|iniciativa|proyecto)", r"apertur(a|ar|ara|o|an)", r"estren(a|o|ara|an|ando)", r"habilit(a|o|ara|an|ando)", r"disponible", r"mejor(a|o|an|ando)", r"optimiza|amplia|expande", r"alianz(a|as)|acuerd(o|a|os)|convenio(s)?|memorando(s)?|joint\s+venture|colaboraci[oó]n(es)?|asociaci[oó]n(es)?|partnership(s)?|fusi[oó]n(es)?|integraci[oó]n(es)?", r"crecimi?ento|aument(a|o|an|ando)", r"gananci(a|as)|utilidad(es)?|benefici(o|os)", r"expansion|crece|crecer", r"inversion|invierte|invertir", r"innova(cion|dor|ndo)|moderniza", r"exito(so|sa)?|logr(o|os|a|an|ando)", r"reconoci(miento|do|da)|premi(o|os|ada)", r"lidera(zgo)?|lider", r"consolida|fortalece", r"oportunidad(es)?|potencial", r"solucion(es)?|resuelve", r"eficien(te|cia)", r"calidad|excelencia", r"satisfaccion|complace", r"confianza|credibilidad", r"sostenible|responsable", r"compromiso|apoya|apoyar", r"patrocin(io|a|ador|an|ando)|auspic(ia|io|iador)", r"gana(r|dor|dora|ndo)?|triunf(a|o|ar|ando)", r"destaca(r|do|da|ndo)?", r"supera(r|ndo|cion)?", r"record|hito|milestone", r"avanza(r|do|da|ndo)?", r"benefici(a|o|ando|ar|ando)", r"importante(s)?", r"prioridad", r"bienestar", r"garantizar", r"seguridad", r"atencion", r"expres(o|ó|ando)", r"señala(r|do|ando)", r"ratific(a|o|ando|ar)"]
NEG_VARIANTS = [r"demanda|denuncia|sanciona|multa|investiga|critica", r"cae|baja|pierde|crisis|quiebra|default", r"fraude|escandalo|irregularidad", r"fall(a|o|os)|interrumpe|suspende|cierra|renuncia|huelga", r"filtracion|ataque|phishing|hackeo|incumple|boicot|queja|reclamo|deteriora", r"problema(s|tica|tico)?|dificultad(es)?", r"retras(o|a|ar|ado)", r"perdida(s)?|deficit", r"conflict(o|os)?|disputa(s)?", r"rechaz(a|o|ar|ado)", r"negativ(o|a|os|as)", r"preocupa(cion|nte|do)?", r"alarma(nte)?|alerta", r"riesgo(s)?|amenaza(s)?"]
CRISIS_KEYWORDS = re.compile(r"\b(crisis|emergencia|desastre|deslizamiento|inundaci[oó]n|afectaciones|damnificados|tragedia|zozobra|alerta)\b", re.IGNORECASE)
RESPONSE_VERBS = re.compile(r"\b(atiend(e|en|iendo)|activ(a|o|ando)|decret(a|o|ando)|responde(r|iendo)|trabaj(a|ando)|lidera(ndo)?|enfrenta(ndo)?|gestiona(ndo)?|declar(o|a|ando)|anunci(a|o|ando))\b", re.IGNORECASE)
POS_PATTERNS = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in POS_VARIANTS]
NEG_PATTERNS = [re.compile(rf"\b(?:{p})\b", re.IGNORECASE) for p in NEG_VARIANTS]


# ======================================
# Estilos CSS
# ======================================
def load_custom_css():
    st.markdown(
        """
        <style>
        :root { --primary-color: #005A3A; --secondary-color: #B38612; --card-bg: #ffffff; --shadow-light: 0 2px 4px rgba(0,0,0,0.1); --border-radius: 12px; }
        .main-header { background: linear-gradient(135deg, var(--primary-color) 0%, var(--secondary-color) 100%); color: white; padding: 2rem; border-radius: var(--border-radius); text-align: center; font-size: 2.2rem; font-weight: 800; margin-bottom: 1.5rem; box-shadow: var(--shadow-light); }
        .subtitle { text-align: center; color: #666; font-size: 1.1rem; margin: -1rem 0 2rem 0; }
        .metric-card { background: var(--card-bg); padding: 1.2rem; border-radius: var(--border-radius); box-shadow: var(--shadow-light); text-align: center; border: 1px solid #e0e0e0; }
        .metric-value { font-size: 2rem; font-weight: bold; color: var(--primary-color); }
        .metric-label { font-size: 0.9rem; color: #666; text-transform: uppercase; }
        .success-card { background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%); padding: 1.5rem; border-radius: var(--border-radius); border: 1px solid #28a745; margin: 1rem 0; box-shadow: var(--shadow-light); }
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

def call_with_retries(api_func, *args, **kwargs):
    max_retries = 3; delay = 1
    for attempt in range(max_retries):
        try: return api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            time.sleep(delay); delay *= 2

async def acall_with_retries(api_func, *args, **kwargs):
    max_retries = 3; delay = 1
    for attempt in range(max_retries):
        try: return await api_func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1: raise e
            await asyncio.sleep(delay); delay *= 2

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

def limpiar_tema_geografico(tema: str, marca: str, aliases: List[str]) -> str:
    if not tema: return "Sin tema"
    tema_lower = tema.lower()
    all_brand_names = [marca.lower()] + [alias.lower() for alias in aliases if alias]
    for brand_name in all_brand_names:
        tema_lower = re.sub(rf'\b{re.escape(brand_name)}\b', '', tema_lower, flags=re.IGNORECASE)
        tema_lower = re.sub(rf'\b{re.escape(unidecode(brand_name))}\b', '', tema_lower, flags=re.IGNORECASE)
    for ciudad in CIUDADES_COLOMBIA:
        tema_lower = re.sub(rf'\b{re.escape(ciudad)}\b', '', tema_lower, flags=re.IGNORECASE)
    for gentilicio in GENTILICIOS_COLOMBIA:
        tema_lower = re.sub(rf'\b{re.escape(gentilicio)}\b', '', tema_lower, flags=re.IGNORECASE)
    frases_geograficas = ["en colombia", "de colombia", "del pais", "en el pais", "nacional", "colombiano", "colombiana", "colombianos", "colombianas", "territorio nacional"]
    for frase in frases_geograficas:
        tema_lower = re.sub(rf'\b{re.escape(frase)}\b', '', tema_lower, flags=re.IGNORECASE)
    palabras = [p.strip() for p in tema_lower.split() if p.strip()]
    if not palabras: return "Sin tema"
    tema_limpio = " ".join(palabras)
    if tema_limpio: tema_limpio = tema_limpio[0].upper() + tema_limpio[1:]
    return limpiar_tema(tema_limpio)

def string_norm_label(s: str) -> str:
    if not s: return ""
    s = unidecode(s.lower())
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return " ".join([t for t in s.split() if t not in STOPWORDS_ES])

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
    return re.sub(r"\s*\|\s*[\w\s]+$", "", str(title)).strip()

def corregir_texto(text: Any) -> Any:
    if not isinstance(text, str): return text
    text = re.sub(r"(<br>|\[\.\.\.\]|\s+)", " ", text).strip()
    match = re.search(r"[A-ZÁÉÍÓÚÑ]", text)
    if match: text = text[match.start():]
    if text and not text.endswith("..."): text = text.rstrip(".") + "..."
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

def simhash(texto: str) -> int:
    if not texto: return 0
    toks = string_norm_label(texto).split()
    if not toks: return 0
    bits = [0] * 64
    for tok in toks:
        hv = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16) & ((1 << 64) - 1)
        for i in range(64): bits[i] += 1 if (hv >> i) & 1 else -1
    v = 0
    for i in range(64):
        if bits[i] >= 0: v |= (1 << i)
    return v

def hamdist(a: int, b: int) -> int:
    return bin(a ^ b).count('1')

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

def agrupar_por_titulo_similar(titulos: List[str]) -> Dict[int, List[int]]:
    gid, grupos, used = 0, {}, set()
    norm_titles = [normalize_title_for_comparison(t) for t in titulos]
    for i in range(len(norm_titles)):
        if i in used or not norm_titles[i]: continue
        grupo_actual = [i]
        used.add(i)
        for j in range(i + 1, len(norm_titles)):
            if j in used or not norm_titles[j]: continue
            if SequenceMatcher(None, norm_titles[i], norm_titles[j]).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                grupo_actual.append(j)
                used.add(j)
        if len(grupo_actual) >= 2: grupos[gid], gid = grupo_actual, gid + 1
    return grupos

def agrupar_por_resumen_puro(resumenes: List[str]) -> Dict[int, List[int]]:
    gid, grupos, used = 0, {}, set()
    norm = [string_norm_label(r or "") for r in resumenes]
    hashes = [simhash(r or "") for r in norm]
    for i in range(len(norm)):
        if i in used or not norm[i]: continue
        grupo = [i]
        used.add(i)
        for j in range(i + 1, len(norm)):
            if j in used or not norm[j]: continue
            if hamdist(hashes[i], hashes[j]) <= 8 and SequenceMatcher(None, norm[i], norm[j]).ratio() >= 0.92:
                grupo.append(j)
                used.add(j)
        if len(grupo) >= 2:
            grupos[gid] = grupo
            gid += 1
    return grupos

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
class ClasificadorTono:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca = marca
        self.aliases = aliases or []

    async def _llm_refuerzo(self, texto: str) -> Dict[str, str]:
        aliases_str = ", ".join(self.aliases) if self.aliases else "ninguno"
        prompt = (
            f"Analice ÚNICAMENTE el sentimiento hacia la marca '{self.marca}' (y sus alias: {aliases_str}), NO el sentimiento general del texto. "
            "Determine el 'tono' (Positivo, Negativo, Neutro) y una 'justificacion' breve (máx 6 palabras) en formato JSON. "
            "Considere positivo: acuerdos, premios, o la respuesta proactiva a una crisis. "
            f"Texto: {texto[:MAX_TOKENS_PROMPT_TXT]}\n"
            'Responda en JSON: {"tono":"...", "justificacion":"..."}'
        )
        try:
            resp = await acall_with_retries(
                openai.ChatCompletion.acreate,
                model=OPENAI_MODEL_CLASIFICACION,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=60,
                temperature=0.0,
                response_format={"type": "json_object"}
            )
            data = json.loads(resp.choices[0].message.content.strip())
            tono = str(data.get("tono", "Neutro")).title()
            return {"tono": tono if tono in ["Positivo","Negativo","Neutro"] else "Neutro", "justificacion": data.get("justificacion", "Análisis LLM")}
        except Exception:
            return {"tono": "Neutro", "justificacion": "Fallo de refuerzo LLM"}

    async def _clasificar_grupo_async(self, texto_representante: str, semaphore: asyncio.Semaphore):
        async with semaphore:
            t = unidecode(texto_representante.lower())
            brand_re = re.compile(r"\b(" + "|".join([re.escape(unidecode(n.strip().lower())) for n in [self.marca] + self.aliases if n.strip()]) + r")\b")
            
            pos_hits = sum(1 for p in POS_PATTERNS if re.search(rf"{brand_re.pattern}.{{0,{WINDOW}}}{p.pattern}|{p.pattern}.{{0,{WINDOW}}}{brand_re.pattern}", t, re.IGNORECASE))
            neg_hits = sum(1 for p in NEG_PATTERNS if re.search(rf"{brand_re.pattern}.{{0,{WINDOW}}}{p.pattern}|{p.pattern}.{{0,{WINDOW}}}{brand_re.pattern}", t, re.IGNORECASE))
            
            is_crisis_response = bool(CRISIS_KEYWORDS.search(t)) and bool(re.search(rf"{brand_re.pattern}.{{0,50}}{RESPONSE_VERBS.pattern}", t, re.IGNORECASE))
            
            if is_crisis_response: return {"tono": "Positivo", "justificacion": "Respuesta activa a crisis"}
            if pos_hits > neg_hits and pos_hits > 0: return {"tono": "Positivo", "justificacion": "Acción favorable"}
            if neg_hits > pos_hits and neg_hits > 0: return {"tono": "Negativo", "justificacion": "Hecho adverso"}
            
            return await self._llm_refuerzo(texto_representante)

class ClasificadorTema:
    def __init__(self, marca: str, aliases: List[str]):
        self.marca, self.aliases = marca, aliases or []

    def _generar_subtema_para_grupo(self, textos_muestra: List[str]) -> str:
        prompt = (f"Genere un subtema específico y preciso (2-6 palabras) para estas noticias. No incluya la marca '{self.marca}', ciudades o gentilicios de Colombia.\n"
                  f"Textos:\n---\n" + "\n---\n".join([m[:500] for m in textos_muestra]) + '\n---\nResponda solo en JSON: {"subtema":"..."}')
        try:
            resp = call_with_retries(openai.ChatCompletion.create, model=OPENAI_MODEL_CLASIFICACION, messages=[{"role": "user", "content": prompt}], max_tokens=40, temperature=0.05, response_format={"type": "json_object"})
            data = json.loads(resp.choices[0].message.content.strip())
            return limpiar_tema_geografico(limpiar_tema(data.get("subtema", "Sin tema")), self.marca, self.aliases)
        except Exception:
            return limpiar_tema(" ".join(string_norm_label(" ".join(textos_muestra)).split()[:4]) or "Actividad Empresarial")

def consolidar_temas(subtemas: List[str], p_bar) -> List[str]:
    p_bar.progress(0.6, text=f"📊 Contando y filtrando subtemas para consolidación...")
    subtema_counts = Counter(st for st in subtemas if st != "Sin tema")
    
    subtemas_a_clusterizar = [st for st, count in subtema_counts.items() if count > 1]
    singletons = [st for st, count in subtema_counts.items() if count == 1]
    
    mapa_subtema_a_tema = {st: st for st in singletons}
    mapa_subtema_a_tema["Sin tema"] = "Sin tema"

    if not subtemas_a_clusterizar or len(subtemas_a_clusterizar) < NUM_TEMAS_PRINCIPALES:
        p_bar.progress(1.0, "ℹ️ No hay suficientes grupos de subtemas para consolidar. Usando subtemas como temas.")
        for st in subtemas_a_clusterizar: mapa_subtema_a_tema[st] = st
        return [mapa_subtema_a_tema.get(st, st) for st in subtemas]

    p_bar.progress(0.7, f"🔄 Agrupando {len(subtemas_a_clusterizar)} subtemas frecuentes...")
    emb_subtemas = {st: get_embedding(st) for st in subtemas_a_clusterizar}
    subtemas_validos = [st for st, emb in emb_subtemas.items() if emb is not None]
    
    if len(subtemas_validos) < NUM_TEMAS_PRINCIPALES:
        p_bar.progress(1.0, "ℹ️ No hay suficientes subtemas con embeddings para consolidar.")
        for st in subtemas_a_clusterizar: mapa_subtema_a_tema[st] = st
        return [mapa_subtema_a_tema.get(st, st) for st in subtemas]

    emb_matrix = np.array([emb_subtemas[st] for st in subtemas_validos])
    clustering = AgglomerativeClustering(n_clusters=NUM_TEMAS_PRINCIPALES, metric="cosine", linkage="average").fit(emb_matrix)
    
    mapa_cluster_a_subtemas = defaultdict(list)
    for i, label in enumerate(clustering.labels_):
        mapa_cluster_a_subtemas[label].append(subtemas_validos[i])

    p_bar.progress(0.8, "🧠 Generando nombres para los temas principales...")
    for cluster_id, lista_subtemas in mapa_cluster_a_subtemas.items():
        prompt = (
            "Eres un analista de medios experto. A partir de la siguiente lista de subtemas, genera un nombre de TEMA principal (2-4 palabras) que los agrupe de forma lógica para un informe ejecutivo. Evita términos vagos como 'Noticias', 'Anuncios' o 'Actividades'.\n"
            "Ejemplo: Si los subtemas son 'Apertura nueva sucursal', 'Resultados financieros Q3', un buen tema es 'Expansión y Finanzas'.\n\n"
            f"Subtemas a agrupar: {', '.join(lista_subtemas[:12])}\n\n"
            "Responde únicamente con el nombre del TEMA principal."
        )
        try:
            resp = call_with_retries(openai.ChatCompletion.create, model=OPENAI_MODEL_CLASIFICACION, messages=[{"role": "user", "content": prompt}], max_tokens=20, temperature=0.2)
            tema_principal = limpiar_tema(resp.choices[0].message.content.strip().replace('"', ''))
        except Exception:
            tema_principal = max(lista_subtemas, key=len)
        
        for subtema in lista_subtemas:
            mapa_subtema_a_tema[subtema] = tema_principal
    
    p_bar.progress(1.0, "✅ Consolidación de temas completada.")
    return [mapa_subtema_a_tema.get(st, st) for st in subtemas]

# ======================================
# Lógica de Duplicados y Generación de Excel
# ======================================
def detectar_duplicados_avanzado(rows: List[Dict], key_map: Dict[str, str]) -> List[Dict]:
    processed_rows = deepcopy(rows)
    seen_online_url = {}
    seen_broadcast = {}
    online_title_buckets = defaultdict(list)

    for i, row in enumerate(processed_rows):
        if row.get("is_duplicate"): continue

        tipo_medio = normalizar_tipo_medio(str(row.get(key_map.get("tipodemedio"))))
        mencion_norm = norm_key(row.get(key_map.get("menciones")))
        medio_norm = norm_key(row.get(key_map.get("medio")))

        if tipo_medio == "Internet":
            link_info = row.get(key_map.get("link_nota"), {})
            url = link_info.get("url") if isinstance(link_info, dict) else None
            
            if url and mencion_norm:
                key = (url, mencion_norm)
                if key in seen_online_url:
                    row["is_duplicate"] = True
                    row["idduplicada"] = processed_rows[seen_online_url[key]].get(key_map.get("idnoticia"), "")
                    continue 
                else: seen_online_url[key] = i
            
            if medio_norm and mencion_norm:
                online_title_buckets[(medio_norm, mencion_norm)].append(i)
        
        elif tipo_medio in ["Radio", "Televisión"]:
            hora = str(row.get(key_map.get("hora"), "")).strip()
            if mencion_norm and medio_norm and hora:
                key = (mencion_norm, medio_norm, hora)
                if key in seen_broadcast:
                    row["is_duplicate"] = True
                    row["idduplicada"] = processed_rows[seen_broadcast[key]].get(key_map.get("idnoticia"), "")
                else: seen_broadcast[key] = i
    
    for _, indices in online_title_buckets.items():
        if len(indices) < 2: continue
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                idx1, idx2 = indices[i], indices[j]
                if processed_rows[idx1].get("is_duplicate") or processed_rows[idx2].get("is_duplicate"): continue
                titulo1 = normalize_title_for_comparison(processed_rows[idx1].get(key_map.get("titulo")))
                titulo2 = normalize_title_for_comparison(processed_rows[idx2].get(key_map.get("titulo")))
                if titulo1 and titulo2 and SequenceMatcher(None, titulo1, titulo2).ratio() >= SIMILARITY_THRESHOLD_TITULOS:
                    if len(titulo1) < len(titulo2): processed_rows[idx1]["is_duplicate"], processed_rows[idx1]["idduplicada"] = True, processed_rows[idx2].get(key_map.get("idnoticia"), "")
                    else: processed_rows[idx2]["is_duplicate"], processed_rows[idx2]["idduplicada"] = True, processed_rows[idx1].get(key_map.get("idnoticia"), "")
    
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
        for m in m_list or [None]:
            new = deepcopy(base); new[key_map["menciones"]] = m if m else new.get(key_map["menciones"])
            split_rows.append(new)
    
    for idx, row in enumerate(split_rows): row.update({"original_index": idx, "is_duplicate": False})
    
    processed_rows = detectar_duplicados_avanzado(split_rows, key_map)
    
    for row in processed_rows:
        if row["is_duplicate"]: row.update({key_map["tonoai"]: "Duplicada", key_map["tema"]: "Duplicada", key_map["justificaciontono"]: "Noticia duplicada."})
    
    return processed_rows, key_map

def fix_links_by_media_type(row: Dict[str, Any], key_map: Dict[str, str]):
    tkey, ln_key, ls_key = key_map.get("tipodemedio"), key_map.get("link_nota"), key_map.get("link_streaming")
    if not (tkey and ln_key and ls_key): return
    tipo = row.get(tkey, "")
    ln, ls = row.get(ln_key) or {"value": "", "url": None}, row.get(ls_key) or {"value": "", "url": None}
    has_url = lambda x: isinstance(x, dict) and bool(x.get("url"))
    
    if tipo in ["Radio", "Televisión"]: row[ls_key] = {"value": "", "url": None}
    elif tipo == "Internet": row[ln_key], row[ls_key] = ls, ln
    elif tipo in ["Prensa", "Revista"]:
        if not has_url(ln) and has_url(ls): row[ln_key] = ls
        row[ls_key] = {"value": "", "url": None}

def generate_output_excel(all_processed_rows, key_map, include_ai_columns=True):
    out_wb = Workbook()
    out_sheet = out_wb.active
    out_sheet.title = "Resultado"
    base_order = ["ID Noticia","Fecha","Hora","Medio","Tipo de Medio","Seccion - Programa","Region","Titulo","Autor - Conductor","Nro. Pagina","Dimension","Duracion - Nro. Caracteres","CPE","Tier","Audiencia","Tono","Resumen - Aclaracion","Link Nota","Link (Streaming - Imagen)","Menciones - Empresa","ID duplicada"]
    ai_order = ["Tono AI", "Tema", "Justificacion Tono"]
    final_order = base_order[:16] + ai_order + base_order[16:] if include_ai_columns else base_order
    
    numeric_columns = {"ID Noticia", "Nro. Pagina", "Dimension", "Duracion - Nro. Caracteres", "CPE", "Tier", "Audiencia"}
    out_sheet.append(final_order)
    link_style = NamedStyle(name="Hyperlink_Custom", font=Font(color="0000FF", underline="single"))
    if "Hyperlink_Custom" not in out_wb.style_names: out_wb.add_named_style(link_style)
    
    for row_data in all_processed_rows:
        row_data[key_map.get("titulo")] = clean_title_for_output(row_data.get(key_map.get("titulo")))
        row_data[key_map.get("resumen")] = corregir_texto(row_data.get(key_map.get("resumen")))
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
        out_sheet.append(row_to_append)
        for col_idx, url in links_to_add.items():
            cell = out_sheet.cell(row=out_sheet.max_row, column=col_idx)
            cell.hyperlink = url
            cell.style = "Hyperlink_Custom"
            
    output = io.BytesIO()
    out_wb.save(output)
    return output.getvalue()

# ======================================
# Lógica de procesamiento modular
# ======================================
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
        fix_links_by_media_type(row, key_map)
    return all_processed_rows

async def run_ai_analysis_async(rows_to_analyze, key_map, brand_name, brand_aliases):
    df_temp = pd.DataFrame(rows_to_analyze)
    df_temp["resumen_api"] = df_temp[key_map["titulo"]].fillna("").astype(str) + ". " + df_temp[key_map["resumen"]].fillna("").astype(str)
    textos = df_temp["resumen_api"].tolist()
    n = len(textos)

    # --- Agrupación para optimizar llamadas a la API ---
    class DSU:
        def __init__(self, n): self.p = list(range(n))
        def find(self, i):
            if self.p[i] == i: return i
            self.p[i] = self.find(self.p[i]); return self.p[i]
        def union(self, i, j): self.p[self.find(j)] = self.find(i)
    
    dsu = DSU(n)
    with st.spinner("Optimizando... Agrupando noticias similares para reducir llamadas a la API..."):
        grupos_combinados = {**agrupar_textos_similares(textos, SIMILARITY_THRESHOLD_TEMAS), 
                             **agrupar_por_titulo_similar(df_temp[key_map["titulo"]].tolist()), 
                             **agrupar_por_resumen_puro(df_temp[key_map["resumen"]].tolist())}
        for _, idxs in grupos_combinados.items():
            for j in idxs[1:]: dsu.union(idxs[0], j)
    
    comp = defaultdict(list)
    for i in range(n): comp[dsu.find(i)].append(i)
    
    st.info(f"💡 Optimización: Se procesarán {n} noticias en {len(comp)} grupos únicos. Esto reduce significativamente las llamadas a la API.")

    # --- Análisis de Tono y Tema ---
    representantes = {cid: seleccionar_representante(idxs, textos) for cid, idxs in comp.items()}
    
    # Tono
    p_bar_tono = st.progress(0, text="🎯 Analizando Tono...")
    clasif_tono = ClasificadorTono(brand_name, brand_aliases)
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    tasks_tono = [clasif_tono._clasificar_grupo_async(rep_texto, semaphore) for _, rep_texto in representantes.values()]
    resultados_tono_brutos = [await f for i, f in enumerate(asyncio.as_completed(tasks_tono), 1) if p_bar_tono.progress(i / len(tasks_tono), f"🎯 Analizando Tono: {i}/{len(tasks_tono)}")]
    
    # Tema (Subtema -> Tema)
    p_bar_tema = st.progress(0, text="🏷️ Generando Temas...")
    clasif_tema = ClasificadorTema(brand_name, brand_aliases)
    mapa_subtemas_grupo = {cid: clasif_tema._generar_subtema_para_grupo([rep_texto]) for cid, (_, rep_texto) in representantes.items()}
    p_bar_tema.progress(0.5, "Consolidando temas principales...")
    
    # Mapear subtemas a todas las filas y luego consolidar
    subtemas_flat = [None] * n
    for cid, idxs in comp.items():
        subtema = mapa_subtemas_grupo.get(cid, "Sin tema")
        for i in idxs: subtemas_flat[i] = subtema
    temas_finales_flat = consolidar_temas(subtemas_flat, p_bar_tema)

    # --- Ensamblar resultados ---
    resultados_tono_por_grupo = {list(representantes.keys())[i]: res for i, res in enumerate(resultados_tono_brutos)}
    for cid, idxs in comp.items():
        res_tono = resultados_tono_por_grupo.get(cid, {"tono": "Neutro", "justificacion": "Sin datos"})
        for i in idxs:
            original_idx = df_temp.index[i]
            df_temp.loc[original_idx, key_map["tonoai"]] = res_tono["tono"]
            df_temp.loc[original_idx, key_map["justificaciontono"]] = res_tono["justificacion"]
            df_temp.loc[original_idx, key_map["tema"]] = temas_finales_flat[i]
    
    return df_temp

# ======================================
# Interfaz de Usuario (UI)
# ======================================
def main():
    load_custom_css()
    if not check_password(): return

    st.markdown('<div class="main-header">🎓 Sistema de Análisis de Noticias para la Universidad Nacional</div>', unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Módulo 1: Deduplicación y Mapeo", "Módulo 2: Análisis de Tono y Tema con IA"])

    # --- Módulo 1: Deduplicación ---
    with tab1:
        st.markdown("### ⚙️ Módulo 1: Deduplicación y Mapeo")
        st.markdown("Este módulo procesa el archivo Dossier, **separa las menciones, detecta noticias duplicadas**, aplica los mapeos de región/internet y corrige los hipervínculos. **No utiliza inteligencia artificial**.")
        
        with st.form("deduplication_form"):
            dossier_file_d = st.file_uploader("**1. Dossier Principal** (.xlsx)", type=["xlsx"], key="dossier_d")
            region_file_d = st.file_uploader("**2. Mapeo de Región** (.xlsx)", type=["xlsx"], key="region_d")
            internet_file_d = st.file_uploader("**3. Mapeo Internet** (.xlsx)", type=["xlsx"], key="internet_d")

            if st.form_submit_button("🚀 **PROCESAR DEDUPLICACIÓN**", use_container_width=True, type="primary"):
                if not all([dossier_file_d, region_file_d, internet_file_d]):
                    st.error("❌ Por favor, cargue los tres archivos requeridos.")
                else:
                    with st.spinner("Procesando..."):
                        all_processed_rows, key_map = run_base_logic(load_workbook(dossier_file_d, data_only=True).active)
                        all_processed_rows = process_mappings_and_links(all_processed_rows, key_map, region_file_d, internet_file_d)
                        
                        st.session_state["output_data_d"] = generate_output_excel(all_processed_rows, key_map, include_ai_columns=False)
                        st.session_state["output_filename_d"] = f"Dossier_Deduplicado_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
                        st.session_state["deduplication_complete"] = True
                        st.rerun()

        if st.session_state.get("deduplication_complete"):
            st.success("✅ Proceso de deduplicación completado.")
            st.download_button("📥 **DESCARGAR REPORTE DEDUPLICADO**", data=st.session_state.output_data_d, file_name=st.session_state.output_filename_d, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")
            if st.button("Nueva Deduplicación", use_container_width=True):
                keys_to_keep = ["password_correct"]
                for key in list(st.session_state.keys()):
                    if key not in keys_to_keep: del st.session_state[key]
                st.rerun()

    # --- Módulo 2: Análisis con IA ---
    with tab2:
        st.markdown("### 🧠 Módulo 2: Análisis de Tono y Tema con IA")
        st.markdown("Este módulo realiza el **proceso completo**: deduplicación, mapeos, corrección de links y, adicionalmente, **analiza el Tono y genera un Tema consolidado** para cada noticia única usando IA.")
        
        with st.form("ai_analysis_form"):
            dossier_file_ai = st.file_uploader("**1. Dossier Principal** (.xlsx)", type=["xlsx"], key="dossier_ai")
            region_file_ai = st.file_uploader("**2. Mapeo de Región** (.xlsx)", type=["xlsx"], key="region_ai")
            internet_file_ai = st.file_uploader("**3. Mapeo Internet** (.xlsx)", type=["xlsx"], key="internet_ai")
            
            st.markdown("#### Configuración de Marca")
            brand_name = st.text_input("**Marca Principal a analizar**", placeholder="Ej: Universidad Nacional")
            brand_aliases_text = st.text_area("**Alias y voceros de la marca** (separados por ;)", placeholder="Ej: UNAL;Dolly Montoya", height=80)

            if st.form_submit_button("🚀 **INICIAR ANÁLISIS COMPLETO CON IA**", use_container_width=True, type="primary"):
                if not all([dossier_file_ai, region_file_ai, internet_file_ai, brand_name.strip()]):
                    st.error("❌ Faltan archivos o el nombre de la marca.")
                else:
                    try: openai.api_key = st.secrets["OPENAI_API_KEY"]; openai.aiosession.set(None)
                    except Exception: st.error("❌ Error: OPENAI_API_KEY no encontrada en los Secrets de Streamlit."); st.stop()
                    
                    aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                    
                    s1 = st.status("📋 **Paso 1/3:** Limpieza, duplicados y mapeos...", expanded=True)
                    with s1:
                        all_processed_rows, key_map = run_base_logic(load_workbook(dossier_file_ai, data_only=True).active)
                        all_processed_rows = process_mappings_and_links(all_processed_rows, key_map, region_file_ai, internet_file_ai)
                        st.write("✅ Mapeos y limpieza completados.")
                    s1.update(label="✅ **Paso 1/3:** Base de datos preparada", state="complete")

                    rows_to_analyze = [row for row in all_processed_rows if not row.get("is_duplicate")]
                    
                    if not rows_to_analyze:
                        st.warning("No se encontraron noticias únicas para analizar con IA.")
                        output_data = generate_output_excel(all_processed_rows, key_map, include_ai_columns=True)
                    else:
                        s2 = st.status(f"🧠 **Paso 2/3:** Analizando Tono y Tema para {len(rows_to_analyze)} noticias únicas...", expanded=True)
                        with s2:
                            df_analyzed = asyncio.run(run_ai_analysis_async(rows_to_analyze, key_map, brand_name, aliases))
                            results_map = df_analyzed.set_index("original_index").to_dict("index")
                            for row in all_processed_rows:
                                if not row.get("is_duplicate"): row.update(results_map.get(row["original_index"], {}))
                        s2.update(label="✅ **Paso 2/3:** Análisis con IA completado", state="complete")

                    s3 = st.status("📊 **Paso 3/3:** Generando informe final...", expanded=True)
                    with s3:
                        output_data = generate_output_excel(all_processed_rows, key_map, include_ai_columns=True)
                    s3.update(label="✅ **Paso 3/3:** Informe generado", state="complete")

                    st.session_state["output_data_ai"] = output_data
                    st.session_state["output_filename_ai"] = f"Informe_IA_{brand_name.replace(' ', '_')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
                    st.session_state["ai_analysis_complete"] = True
                    st.rerun()

        if st.session_state.get("ai_analysis_complete"):
            st.success("🎉 Análisis completo con IA finalizado exitosamente.")
            st.download_button("📥 **DESCARGAR INFORME COMPLETO CON IA**", data=st.session_state.output_data_ai, file_name=st.session_state.output_filename_ai, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")
            if st.button("Nuevo Análisis con IA", use_container_width=True):
                keys_to_keep = ["password_correct"]
                for key in list(st.session_state.keys()):
                    if key not in keys_to_keep: del st.session_state[key]
                st.rerun()
                
    st.markdown("<hr><div style='text-align:center;color:#666;font-size:0.9rem;'><p>Sistema de Análisis de Noticias v5.0 | Adaptado para la Universidad Nacional</p></div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()

"""
Clasificación de TONO y TEMA con OpenAI (familia GPT Luna).

Módulo sin dependencias de Streamlit para poder probarlo de forma aislada.

Diferencias clave frente a la versión anterior:
  * Modelo gpt-6-luna (con respaldo gpt-5.6-luna). gpt-4.1-nano se apagó el 2026-10-23.
  * Salida estructurada (json_schema estricto con enum): el modelo SOLO puede devolver
    un tema de la taxonomía o un tono válido; ya no hay "fallback semántico".
  * Los errores NUNCA se convierten en "Neutro" / "Vida universitaria". Se registran,
    se devuelven como "Sin clasificar" y la app los muestra al usuario.
  * Verificación previa (preflight) del modelo antes de procesar el dossier.
  * Cada noticia se clasifica con su propio texto (solo se deduplican textos idénticos).
  * Límite de tokens por minuto (TPM): limitador propio + pausa global y reintentos con
    espera exponencial ante 429, y rondas finales de reintento para lo que haya fallado.
  * Voceros con tono fijo (p. ej. el rector José Ismael Peña => Positivo).
  * El TEMA nunca queda "Sin clasificar": si tras los reintentos la API no responde para una
    noticia, se asigna por palabras clave y se informa como aviso.
"""
import asyncio
import collections
import hashlib
import json
import random
import re
import time
from typing import Callable, Dict, List, Optional, Sequence

from openai import (
    AsyncOpenAI,
    APIConnectionError,
    APITimeoutError,
    AuthenticationError,
    InternalServerError,
    RateLimitError,
    BadRequestError,
    NotFoundError,
    PermissionDeniedError,
)
from unidecode import unidecode

# ======================================
# Modelo
# ======================================
# gpt-4.1-nano fue retirado por OpenAI el 2026-10-23 (reemplazo oficial: gpt-5.6-luna).
MODELO_PRINCIPAL = "gpt-6-luna"
MODELO_RESPALDO = "gpt-5.6-luna"
CADENA_MODELOS = [MODELO_PRINCIPAL, MODELO_RESPALDO]

# USD por 1M de tokens (entrada, salida). Verificar en la página de precios de OpenAI.
PRECIOS_MODELO = {
    "gpt-6-luna": (0.10, 0.50),
    "gpt-5.6-luna": (1.00, 6.00),   # fuentes discrepan (0.20/1.20 vs 1.00/6.00): se usa el valor alto
}
PRECIO_DESCONOCIDO = (1.00, 6.00)

TEXTO_MAX_TONO = 3500
TEXTO_MAX_TEMA = 3500

# Límite de tokens por minuto de la organización (gpt-6-luna: 2.000.000 TPM). Se usa un margen
# del 80 % porque la organización puede tener otros procesos consumiendo la misma cuota.
TPM_LIMITE_DEFECTO = 1_600_000
MAX_REINTENTOS_429 = 8
RONDAS_REINTENTO = 2          # rondas extra (con menos concurrencia) para lo que haya fallado
CONCURRENCIA_REINTENTO = 4

SIN_CLASIFICAR = "Sin clasificar"
TEMA_RESIDUAL = "Vida universitaria"

# Voceros cuyas intervenciones siempre son POSITIVAS para la reputación de la institución.
VOCEROS_POSITIVOS_DEFECTO = ("José Ismael Peña",)
TONOS_VALIDOS = ("Positivo", "Negativo", "Neutro")


class ErrorClasificador(Exception):
    """Error irrecuperable de la API (modelo no disponible, credenciales, red...)."""


# ======================================
# Taxonomía fija (18 generales + 5 específicas)
# ======================================
TAXONOMIA_TEMAS: Dict[str, str] = {
    # ── 18 categorías generales ──────────────────────────────────────────────
    "Vida universitaria":
        "Actividades del campus, eventos institucionales, ceremonias y grados, deportes, "
        "convivencia y cotidianidad de la comunidad universitaria. CATEGORÍA RESIDUAL: "
        "úsala solo si ninguna otra describe mejor el asunto central.",
    "Formación académica":
        "Programas de pregrado, posgrado y educación continua, nuevas carreras, admisiones, "
        "oferta curricular, procesos pedagógicos y calidad de la enseñanza.",
    "Investigación y ciencia":
        "Proyectos de investigación, publicaciones científicas, grupos de investigación, "
        "hallazgos y producción de conocimiento (cuando NO son de salud ni de ambiente).",
    "Innovación y transferencia tecnológica":
        "Spin-offs, patentes, emprendimiento universitario, prototipos, alianzas "
        "industria-academia, transferencia de resultados de investigación al sector productivo.",
    "Extensión y proyección social":
        "Programas de extensión, trabajo comunitario, voluntariado, vinculación con "
        "territorios, impacto social de la universidad en poblaciones vulnerables.",
    "Internacionalización":
        "Convenios internacionales, movilidad académica, intercambios estudiantiles y "
        "docentes, redes globales, doble titulación y cooperación exterior.",
    "Política y gestión institucional":
        "Gobierno universitario, elecciones y designación de rector y directivos, reformas "
        "estatutarias, decisiones del Consejo Superior o Académico, planes de desarrollo.",
    "Financiamiento y presupuesto":
        "Presupuesto universitario, transferencias del Estado, déficit fiscal, recursos para "
        "las universidades públicas, autonomía financiera y rendición de cuentas.",
    "Infraestructura y campus":
        "Construcción, renovación y mantenimiento de edificios, laboratorios, bibliotecas, "
        "sedes regionales y espacios físicos universitarios.",
    "Tecnología e innovación digital":
        "Transformación digital, plataformas virtuales, educación en línea, inteligencia "
        "artificial, ciberseguridad y herramientas tecnológicas institucionales.",
    "Bienestar universitario":
        "Salud mental, apoyo psicosocial, alimentación, becas, subsidios, permanencia "
        "estudiantil y programas de apoyo socioeconómico a estudiantes.",
    "Diversidad e inclusión":
        "Equidad de género, inclusión de personas con discapacidad, enfoque diferencial, "
        "comunidades étnicas, LGBTQ+ y políticas antidiscriminación.",
    "Medio ambiente y sostenibilidad":
        "Gestión ambiental del campus, investigación en ecología, cambio climático, "
        "biodiversidad, energías limpias y compromisos de sostenibilidad.",
    "Relaciones con el Estado y política pública":
        "Interacción con el gobierno nacional y territorial, asesoría a políticas públicas, "
        "relación con el Congreso, Ministerio de Educación y entidades estatales.",
    "Conflicto, orden público y seguridad":
        "Protestas, bloqueos, tropeles, cierres de campus, vandalismo, orden público, "
        "seguridad universitaria, agresiones y conflictos internos.",
    "Cultura y patrimonio":
        "Museos, archivos, colecciones, expresiones artísticas, patrimonio cultural de la "
        "universidad, festivales y actividades culturales.",
    "Salud pública y ciencias de la salud":
        "Facultades de medicina, enfermería y odontología, investigación médica o clínica, "
        "hospitales universitarios, epidemiología y respuesta a emergencias sanitarias.",
    "Opinión y análisis experto":
        "Declaraciones de académicos como fuente experta sobre coyunturas ajenas a la "
        "universidad, columnas de opinión y posicionamiento público ante temas nacionales.",

    # ── 5 categorías específicas ─────────────────────────────────────────────
    "Reforma a la educación superior":
        "Debates, proyectos de ley y políticas sobre el sistema de educación superior en "
        "Colombia, reforma a la Ley 30, financiación del sistema y autonomía.",
    "Acreditación y calidad educativa":
        "Acreditación institucional y de programas, rankings universitarios (QS, THE, "
        "ScimagoIR...), evaluación de calidad, pares académicos y certificaciones.",
    "Propiedad intelectual y derechos de autor":
        "Conflictos sobre patentes, derechos de publicación, acceso abierto, uso de obras "
        "académicas y política de propiedad intelectual universitaria.",
    "Relación universidad-empresa":
        "Contratos con el sector privado, centros de investigación financiados por empresas, "
        "conflictos de interés, consultoría académica y spin-offs empresariales.",
    "Crisis o emergencia institucional":
        "Crisis GRAVES y sostenidas: paros prolongados, intervenciones externas, escándalos "
        "administrativos, crisis de gobernanza o colapso de servicios.",
}

NOMBRES_TEMAS: List[str] = list(TAXONOMIA_TEMAS.keys())

_CRITERIOS_TEMA = """\
CRITERIOS DE DECISIÓN (aplícalos en orden):
1. Clasifica por el ASUNTO CENTRAL de la noticia (lo que dice el titular y el primer párrafo),
   no por temas secundarios ni por el simple hecho de que ocurra en la Universidad Nacional.
2. "Vida universitaria" es residual: úsala únicamente si ninguna otra categoría encaja.
3. Las categorías ESPECÍFICAS ganan a las generales cuando aplican con claridad:
   - proyecto de ley, Ley 30 o reforma del sistema de educación superior → "Reforma a la educación superior"
   - acreditación, ranking o evaluación de calidad → "Acreditación y calidad educativa"
   - patentes, derechos de autor, acceso abierto → "Propiedad intelectual y derechos de autor"
   - contratos o alianzas con empresas, conflictos de interés → "Relación universidad-empresa"
   - "Crisis o emergencia institucional" solo si es grave y sostenida (paro prolongado, intervención,
     escándalo de gobernanza, colapso de servicios); una protesta o bloqueo puntual es "Conflicto, orden público y seguridad".
4. Investigación según su dominio: salud/medicina → "Salud pública y ciencias de la salud";
   ecología/clima/biodiversidad → "Medio ambiente y sostenibilidad"; patente, prototipo o spin-off →
   "Innovación y transferencia tecnológica"; cualquier otra → "Investigación y ciencia".
5. Dinero: presupuesto, déficit, recursos del Estado para la universidad → "Financiamiento y presupuesto".
   Becas, subsidios, apoyos alimentarios o permanencia de estudiantes → "Bienestar universitario".
6. Gobierno interno (rector, Consejo Superior, estatutos) → "Política y gestión institucional".
   Relación con Congreso, ministerios o gobierno → "Relaciones con el Estado y política pública".
7. Un profesor de la UNAL opinando o explicando un tema nacional ajeno a la universidad (economía,
   política, salud, seguridad...) → "Opinión y análisis experto". Si habla de SU investigación → regla 4.
8. Arte, museos, música, exposiciones, patrimonio → "Cultura y patrimonio".
   Deportes, eventos, ceremonias, grados → "Vida universitaria".
9. Convenios, movilidad o dobles titulaciones con instituciones extranjeras → "Internacionalización".
10. Obras, edificios, laboratorios o sedes → "Infraestructura y campus".
    IA, plataformas, ciberseguridad o educación virtual → "Tecnología e innovación digital".
11. Comunidades, territorios, voluntariado → "Extensión y proyección social".
    Género, discapacidad, enfoque étnico, LGBTQ+ → "Diversidad e inclusión".
12. Programas, nuevas carreras, admisiones, calidad docente → "Formación académica"."""

_EJEMPLOS_TEMA = """\
EJEMPLOS (titular → tema):
- UNAL abre inscripciones a 12 nuevos programas de posgrado → Formación académica
- Investigadores de la UNAL identifican proteína asociada al Alzheimer → Salud pública y ciencias de la salud
- Grupo de la UNAL desarrolla prótesis de bajo costo y solicita patente → Innovación y transferencia tecnológica
- Estudiantes bloquean la calle 26 en protesta por el presupuesto → Conflicto, orden público y seguridad
- Rector pide al Congreso más recursos para las universidades públicas → Financiamiento y presupuesto
- Consejo Superior elige nuevo rector de la UNAL → Política y gestión institucional
- Profesor de la UNAL explica el efecto de la reforma tributaria en la inflación → Opinión y análisis experto
- UNAL sube 20 puestos en el ranking QS → Acreditación y calidad educativa
- Torneo interfacultades de fútbol reúne a 800 estudiantes → Vida universitaria
- UNAL entrega 500 apoyos alimentarios a estudiantes de bajos recursos → Bienestar universitario
- Comisión Sexta de la Cámara debate el proyecto de reforma a la Ley 30 → Reforma a la educación superior
- UNAL firma convenio de doble titulación con universidad alemana → Internacionalización"""


def _lineas_taxonomia() -> str:
    return "\n".join(
        f'{i:2d}. "{nombre}": {desc}'
        for i, (nombre, desc) in enumerate(TAXONOMIA_TEMAS.items(), 1)
    )


SISTEMA_TEMA = (
    "Eres un analista de monitoreo de medios de la Universidad Nacional de Colombia (UNAL). "
    "Clasificas cada noticia en EXACTAMENTE UNA de estas 23 categorías:\n\n"
    f"{_lineas_taxonomia()}\n\n{_CRITERIOS_TEMA}\n\n{_EJEMPLOS_TEMA}\n\n"
    'Responde solo con JSON: "evidencia" (máx. 25 palabras: el hecho central que justifica la '
    'categoría) y "tema" (nombre exacto de la categoría).'
)

SISTEMA_TONO_TPL = (
    "Eres analista de reputación institucional. Evalúas el efecto de una noticia sobre la imagen "
    "pública de '{marca}'{aliases}.\n\n"
    "Juzga el impacto sobre la reputación de la institución, NO lo triste o alegre que sea el tema.\n\n"
    "POSITIVO: la institución, su comunidad o sus resultados aparecen como logro o aporte: premios, "
    "rankings favorables, hallazgos o desarrollos presentados como avance, becas y apoyos, obras o "
    "programas nuevos, alianzas, impacto social, reconocimiento como referente o fuente autorizada.\n"
    "NEGATIVO: la institución aparece como responsable o víctima directa de algo malo: paros, "
    "protestas o bloqueos con afectación, violencia, acoso o agresiones en el campus, denuncias, "
    "investigaciones, irregularidades o corrupción, déficit o crisis financiera, críticas o "
    "cuestionamientos a la institución o a sus directivas, accidentes, fallas de servicios, caídas en rankings.\n"
    "NEUTRO: información sin carga valorativa (agenda, convocatorias, comunicados, cifras), mención "
    "tangencial o en una lista, o noticia sobre un hecho ajeno donde la institución solo se cita "
    "(por ejemplo, un docente explicando un tema sin valorar a la universidad).\n\n"
    "Reglas: decide por el hecho principal del titular y del primer párrafo. Si hay señales mixtas, "
    "gana el hecho central. Usa NEUTRO solo cuando de verdad no haya valoración; no lo uses como "
    "opción por defecto. Si un tercero sufre el hecho grave y la institución solo informa, es NEUTRO.\n"
    "{regla_voceros}\n"
    "EJEMPLOS:\n"
    "- La UNAL entra al top 5 de universidades de Latinoamérica → Positivo\n"
    "- Estudiantes denuncian acoso por parte de un docente de la UNAL → Negativo\n"
    "- Un profesor de la UNAL, citado como fuente, explica cómo se calcula la inflación → Neutro\n"
    "- Paro estudiantil bloquea el ingreso al campus por tercera semana → Negativo\n"
    "- Investigadores de la UNAL desarrollan un sensor que reduce costos de monitoreo del agua → Positivo\n"
    "- El rector de la UNAL advierte que el país pasó de 900.000 a 450.000 bachilleres y que eso reta "
    "a la educación superior → Positivo (la institución aparece como voz autorizada que analiza un reto del país)\n\n"
    'Responde solo con JSON: "evidencia" (máx. 25 palabras con el hecho que decide el tono) y "tono".'
)


def _esquema(campo: str, valores: Sequence[str]) -> dict:
    return {
        "type": "object",
        "properties": {
            "evidencia": {"type": "string"},
            campo: {"type": "string", "enum": list(valores)},
        },
        "required": ["evidencia", campo],
        "additionalProperties": False,
    }


ESQUEMA_TEMA = _esquema("tema", NOMBRES_TEMAS)
ESQUEMA_TONO = _esquema("tono", TONOS_VALIDOS)


# ======================================
# Utilidades
# ======================================
def _clave_texto(titulo: str, resumen: str) -> str:
    """Clave de deduplicación EXACTA (ignora mayúsculas, tildes y espacios)."""
    base = unidecode(f"{titulo or ''}|{(resumen or '')[:TEXTO_MAX_TEMA]}".lower())
    base = re.sub(r"\s+", " ", base).strip()
    return hashlib.md5(base.encode("utf-8", errors="ignore")).hexdigest()


def _modelo_no_disponible(e: Exception) -> bool:
    if isinstance(e, (NotFoundError, PermissionDeniedError)):
        return True
    if isinstance(e, BadRequestError):
        msg = str(e).lower()
        return getattr(e, "code", None) == "model_not_found" or (
            "model" in msg and any(k in msg for k in ("does not exist", "not found", "deprecated", "shut down", "no longer"))
        )
    return False


def _normalizar(texto: str) -> str:
    return re.sub(r"\s+", " ", unidecode(str(texto or "")).lower()).strip()


def _variantes_vocero(nombre: str) -> List[str]:
    """'José Ismael Peña' -> ['jose ismael pena', 'ismael pena', 'rector pena']."""
    partes = _normalizar(nombre).split()
    if not partes:
        return []
    variantes = {" ".join(partes)}
    if len(partes) >= 3:
        variantes.add(" ".join(partes[1:]))          # sin el primer nombre
    if len(partes) >= 2:
        variantes.add(f"rector {partes[-1]}")         # "el rector Peña"
    return sorted(variantes, key=len, reverse=True)


def menciona_vocero(titulo: str, resumen: str, voceros: Sequence[str]) -> Optional[str]:
    """Devuelve el vocero mencionado en la noticia (o None)."""
    texto = _normalizar(f"{titulo or ''} {resumen or ''}")
    for v in voceros:
        for var in _variantes_vocero(v):
            if re.search(rf"\b{re.escape(var)}\b", texto):
                return v
    return None


# Palabras clave para el respaldo de TEMA (solo si la API no respondió tras todos los reintentos).
# Orden = prioridad (las específicas primero, la residual nunca aparece aquí).
_PALABRAS_TEMA: List[tuple] = [
    ("Reforma a la educación superior", ("ley 30", "reforma a la educacion superior", "reforma de la educacion superior")),
    ("Acreditación y calidad educativa", ("acreditacion", "ranking", "qs ", "scimago", "times higher")),
    ("Propiedad intelectual y derechos de autor", ("derechos de autor", "propiedad intelectual", "acceso abierto")),
    ("Crisis o emergencia institucional", ("intervencion", "escandalo", "crisis institucional", "paro indefinido")),
    ("Conflicto, orden público y seguridad", ("bloqueo", "bloquean", "tropel", "protesta", "disturbio", "esmad", "vandal", "agresion", "seguridad")),
    ("Financiamiento y presupuesto", ("presupuesto", "deficit", "financiacion", "financiamiento", "recursos economicos", "billones", "millones de pesos")),
    ("Relaciones con el Estado y política pública", ("congreso", "ministerio", "ministra", "ministro", "gobierno nacional", "politica publica")),
    ("Política y gestión institucional", ("rector", "consejo superior", "consejo academico", "estatuto", "vicerrector")),
    ("Salud pública y ciencias de la salud", ("salud", "medicina", "hospital", "enfermedad", "virus", "epidemi", "clinic")),
    ("Medio ambiente y sostenibilidad", ("ambiental", "clima", "biodiversidad", "ecolog", "sostenib", "energia")),
    ("Innovación y transferencia tecnológica", ("patente", "spin-off", "spin off", "emprendimiento", "prototipo")),
    ("Relación universidad-empresa", ("empresa", "sector privado", "industria")),
    ("Tecnología e innovación digital", ("inteligencia artificial", "digital", "ciberseguridad", "plataforma virtual", "software")),
    ("Internacionalización", ("internacional", "intercambio", "doble titulacion", "movilidad academica", "extranjer")),
    ("Infraestructura y campus", ("edificio", "obra", "construccion", "infraestructura", "laboratorio", "sede")),
    ("Bienestar universitario", ("beca", "bienestar", "salud mental", "apoyo alimentario", "subsidio", "permanencia")),
    ("Diversidad e inclusión", ("genero", "inclusion", "discapacidad", "indigena", "afro", "lgbt")),
    ("Cultura y patrimonio", ("museo", "cultura", "arte", "patrimonio", "concierto", "exposicion", "festival")),
    ("Extensión y proyección social", ("comunidad", "territorio", "voluntari", "extension")),
    ("Investigación y ciencia", ("investigacion", "investigador", "cientific", "estudio", "hallazgo")),
    ("Formación académica", ("programa", "pregrado", "posgrado", "maestria", "doctorado", "admision", "carrera", "bachiller")),
    ("Opinión y análisis experto", ("profesor", "experto", "docente", "analista", "columna", "opinion")),
]


def tema_por_palabras_clave(titulo: str, resumen: str) -> str:
    """Respaldo determinista: SIEMPRE devuelve un tema de la taxonomía."""
    t = _normalizar(titulo)
    r = _normalizar(resumen)[:TEXTO_MAX_TEMA]
    mejor, mejor_puntaje = TEMA_RESIDUAL, 0
    for tema, claves in _PALABRAS_TEMA:
        puntaje = sum(3 * t.count(k) + r.count(k) for k in claves)
        if puntaje > mejor_puntaje:
            mejor, mejor_puntaje = tema, puntaje
    return mejor


def asegurar_tema(tema: Optional[str], titulo: str = "", resumen: str = "") -> str:
    """Garantiza que el tema pertenezca a la taxonomía."""
    if tema in TAXONOMIA_TEMAS:
        return tema
    return tema_por_palabras_clave(titulo, resumen)


_RE_ESPERA = re.compile(r"try again in\s+((?:\d+(?:\.\d+)?\s*(?:ms|s|m|h)\s*)+)", re.I)


def _espera_sugerida(e: Exception) -> float:
    """Segundos que sugiere la API (cabeceras retry-after o el texto 'try again in 92ms')."""
    resp = getattr(e, "response", None)
    headers = getattr(resp, "headers", None) or {}
    try:
        if headers.get("retry-after-ms"):
            return float(headers["retry-after-ms"]) / 1000
        if headers.get("retry-after"):
            return float(headers["retry-after"])
    except (TypeError, ValueError):
        pass
    m = _RE_ESPERA.search(str(e))
    if not m:
        return 0.0
    total = 0.0
    for num, unidad in re.findall(r"(\d+(?:\.\d+)?)\s*(ms|s|m|h)", m.group(1)):
        total += float(num) * {"ms": 0.001, "s": 1, "m": 60, "h": 3600}[unidad]
    return total


def _es_sin_cuota(e: Exception) -> bool:
    """429 por falta de saldo/cuota: no se arregla reintentando."""
    cuerpo = getattr(e, "body", None)
    code = (cuerpo or {}).get("code") if isinstance(cuerpo, dict) else None
    return getattr(e, "code", None) == "insufficient_quota" or code == "insufficient_quota" \
        or "insufficient_quota" in str(e) or "exceeded your current quota" in str(e).lower()


class LimitadorTPM:
    """Ventana deslizante de 60 s: no deja salir peticiones que superen `limite` tokens/minuto."""

    def __init__(self, limite: Optional[int]):
        self.limite = limite if limite and limite > 0 else None
        self._eventos: collections.deque = collections.deque()
        self._usados = 0
        self._lock = asyncio.Lock()

    async def reservar(self, tokens: int) -> None:
        if self.limite is None:
            return
        tokens = min(tokens, self.limite)
        async with self._lock:
            while True:
                ahora = time.monotonic()
                while self._eventos and ahora - self._eventos[0][0] >= 60:
                    self._usados -= self._eventos.popleft()[1]
                if self._usados + tokens <= self.limite:
                    self._eventos.append((ahora, tokens))
                    self._usados += tokens
                    return
                await asyncio.sleep(max(60 - (ahora - self._eventos[0][0]), 0.05))


def _estimar_tokens(kwargs: dict) -> int:
    chars = sum(len(m["content"]) for m in kwargs["messages"])
    return int(chars / 3) + kwargs.get("max_completion_tokens", 0)


def costo_usd(modelo: Optional[str], tokens_in: int, tokens_out: int) -> float:
    p_in, p_out = PRECIOS_MODELO.get(modelo or "", PRECIO_DESCONOCIDO)
    return tokens_in / 1e6 * p_in + tokens_out / 1e6 * p_out


# ======================================
# Clasificador
# ======================================
class ClasificadorIA:
    def __init__(
        self,
        api_key: str,
        marca: str,
        aliases: Optional[Sequence[str]] = None,
        modelo: Optional[str] = None,
        concurrencia: int = 30,
        base_url: Optional[str] = None,
        voceros_positivos: Optional[Sequence[str]] = None,
        tpm_limite: Optional[int] = TPM_LIMITE_DEFECTO,
        espera_base: float = 1.0,
    ):
        self.api_key = api_key
        self.base_url = base_url
        self.marca = marca.strip()
        self.aliases = [a.strip() for a in (aliases or []) if a.strip()]
        self.concurrencia = concurrencia
        self.voceros = [v.strip() for v in (
            VOCEROS_POSITIVOS_DEFECTO if voceros_positivos is None else voceros_positivos
        ) if v and v.strip()]
        # Siempre Luna: un modelo forzado solo se acepta si pertenece a la familia Luna.
        if modelo and "luna" in modelo.lower():
            self.cadena = [modelo] + [m for m in CADENA_MODELOS if m != modelo]
        else:
            self.cadena = list(CADENA_MODELOS)
        self.modelo_activo: Optional[str] = None
        self.tokens_in = 0
        self.tokens_out = 0
        self.errores: List[str] = []
        self.avisos: List[str] = []
        self.reintentos_429 = 0
        self.tonos_por_vocero = 0
        self._client: Optional[AsyncOpenAI] = None
        self._sin_reasoning_effort = False
        self._tpm = LimitadorTPM(tpm_limite)
        self._espera_base = espera_base
        self._pausa_hasta = 0.0   # pausa GLOBAL: tras un 429 todas las tareas esperan

        alias_txt = f" (también llamada: {', '.join(self.aliases)})" if self.aliases else ""
        if self.voceros:
            regla = (
                "VOCEROS: las intervenciones, declaraciones o análisis de "
                + ", ".join(self.voceros)
                + " (vocero oficial de la institución) son SIEMPRE POSITIVAS, aunque hable de "
                "problemas o retos del país o del sector: la institución aparece como referente.\n"
            )
        else:
            regla = ""
        regla += (
            "Cuando un directivo de la institución (rector, vicerrector, decano) analiza un reto o "
            "problema del país o del sector educativo, NO es negativo para la institución.\n"
        )
        self._sistema_tono = SISTEMA_TONO_TPL.format(
            marca=self.marca, aliases=alias_txt, regla_voceros=regla
        )

    # ── infraestructura ──────────────────────────────────────────────────────
    def _cliente(self) -> AsyncOpenAI:
        if self._client is None:
            # Los 429 los maneja _crear (pausa global + espera exponencial); el SDK solo reintenta 1 vez.
            self._client = AsyncOpenAI(
                api_key=self.api_key, base_url=self.base_url, max_retries=1, timeout=90.0
            )
        return self._client

    async def cerrar(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None

    @property
    def costo(self) -> float:
        return costo_usd(self.modelo_activo or self.cadena[0], self.tokens_in, self.tokens_out)

    async def _esperar_pausa(self) -> None:
        while True:
            falta = self._pausa_hasta - time.monotonic()
            if falta <= 0:
                return
            await asyncio.sleep(falta)

    async def _crear(self, kwargs: dict):
        """chat.completions.create con control de TPM y reintentos ante 429 / 5xx / timeout."""
        intento = 0
        while True:
            await self._esperar_pausa()
            await self._tpm.reservar(_estimar_tokens(kwargs))
            try:
                return await self._cliente().chat.completions.create(**kwargs)
            except RateLimitError as e:
                if _es_sin_cuota(e):
                    raise ErrorClasificador(
                        f"La cuenta de OpenAI no tiene saldo/cuota disponible: {e}"
                    ) from e
                intento += 1
                self.reintentos_429 += 1
                if intento > MAX_REINTENTOS_429:
                    raise
                espera = max(_espera_sugerida(e), self._espera_base * 2 ** (intento - 1))
                espera = min(espera, 60.0) + random.uniform(0, self._espera_base)
                self._pausa_hasta = max(self._pausa_hasta, time.monotonic() + espera)
            except (APITimeoutError, InternalServerError):
                intento += 1
                if intento > 4:
                    raise
                await asyncio.sleep(min(self._espera_base * 2 ** intento, 30.0))

    async def _json(self, sistema: str, usuario: str, nombre: str, esquema: dict, campo: str) -> str:
        """Llama al modelo con salida estructurada y devuelve el valor de `campo`."""
        modelos = self.cadena if self.modelo_activo is None else (
            [self.modelo_activo] + [m for m in self.cadena if m != self.modelo_activo]
        )
        ultimo: Optional[Exception] = None
        for modelo in modelos:
            kwargs = dict(
                model=modelo,
                messages=[{"role": "system", "content": sistema}, {"role": "user", "content": usuario}],
                response_format={
                    "type": "json_schema",
                    "json_schema": {"name": nombre, "strict": True, "schema": esquema},
                },
                max_completion_tokens=500,
            )
            if not self._sin_reasoning_effort:
                kwargs["reasoning_effort"] = "none"   # clasificación: sin "pensar", más rápido y barato
            for _ in range(2):
                try:
                    resp = await self._crear(kwargs)
                except BadRequestError as e:
                    if getattr(e, "param", None) == "reasoning_effort" and "reasoning_effort" in kwargs:
                        kwargs.pop("reasoning_effort")
                        self._sin_reasoning_effort = True
                        continue
                    if _modelo_no_disponible(e):
                        ultimo = e
                        break
                    raise ErrorClasificador(f"Solicitud rechazada por la API ({modelo}): {e}") from e
                except (NotFoundError, PermissionDeniedError) as e:
                    ultimo = e
                    break
                except AuthenticationError as e:
                    raise ErrorClasificador(f"OPENAI_API_KEY inválida o sin permisos: {e}") from e
                except APITimeoutError:
                    raise   # transitorio: se reintenta en las rondas finales
                except APIConnectionError as e:
                    raise ErrorClasificador(f"No hay conexión con la API de OpenAI: {e}") from e

                if resp.usage:
                    self.tokens_in += resp.usage.prompt_tokens or 0
                    self.tokens_out += resp.usage.completion_tokens or 0
                contenido = resp.choices[0].message.content
                if not contenido:
                    raise ValueError(f"respuesta vacía (finish_reason={resp.choices[0].finish_reason})")
                valor = json.loads(contenido).get(campo)
                if valor not in esquema["properties"][campo]["enum"]:
                    raise ValueError(f"valor fuera de catálogo: {valor!r}")
                self.modelo_activo = modelo
                return valor
        raise ErrorClasificador(
            f"Ningún modelo disponible ({', '.join(modelos)}). Último error: {ultimo}"
        )

    # ── verificación previa ──────────────────────────────────────────────────
    async def verificar(self) -> str:
        """Hace una llamada mínima. Lanza ErrorClasificador si el modelo/credenciales fallan."""
        await self._json(
            SISTEMA_TEMA,
            "TÍTULO: Torneo de fútbol interfacultades\nRESUMEN: Más de 800 estudiantes participaron.",
            "tema_ia", ESQUEMA_TEMA, "tema",
        )
        return self.modelo_activo or ""

    # ── clasificación por lotes ──────────────────────────────────────────────
    async def _lote(
        self,
        titulos: Sequence[str],
        resumenes: Sequence[str],
        construir: Callable[[str, str], tuple],
        on_progress: Optional[Callable[[int, int], None]],
        fijos: Optional[Dict[int, str]] = None,
    ) -> List[str]:
        """Clasifica cada texto único. `fijos` = {índice: valor} que no necesitan la API."""
        n = len(titulos)
        fijos = fijos or {}
        claves = [_clave_texto(titulos[i], resumenes[i]) for i in range(n)]
        unicos: Dict[str, int] = {}
        for i, k in enumerate(claves):
            if i not in fijos:
                unicos.setdefault(k, i)

        resultados: Dict[str, str] = {}
        fallos: Dict[str, Exception] = {}
        hechos = 0
        total = len(unicos)

        async def trabajar(sem: asyncio.Semaphore, k: str, i: int, contar: bool):
            nonlocal hechos
            async with sem:
                try:
                    resultados[k] = await self._json(*construir(str(titulos[i] or ""), str(resumenes[i] or "")))
                    fallos.pop(k, None)
                except ErrorClasificador:
                    raise
                except Exception as e:  # error puntual: se reintenta en las rondas finales
                    fallos[k] = e
                if contar:
                    hechos += 1
                    if on_progress:
                        on_progress(hechos, total)

        sem = asyncio.Semaphore(self.concurrencia)
        await asyncio.gather(*(trabajar(sem, k, i, True) for k, i in unicos.items()))

        # Rondas de reintento con menos concurrencia para lo que falló (429 agotados, timeouts...).
        for ronda in range(RONDAS_REINTENTO):
            if not fallos:
                break
            await asyncio.sleep(self._espera_base * 5 * (ronda + 1))
            sem = asyncio.Semaphore(CONCURRENCIA_REINTENTO)
            await asyncio.gather(*(trabajar(sem, k, unicos[k], False) for k in list(fallos)))

        for k, e in fallos.items():
            self.errores.append(f"fila {unicos[k] + 2}: {type(e).__name__}: {e}")
            resultados[k] = SIN_CLASIFICAR
        return [fijos[i] if i in fijos else resultados[claves[i]] for i in range(n)]

    async def clasificar_temas(self, titulos, resumenes, on_progress=None) -> List[str]:
        """Siempre devuelve temas de la taxonomía (nunca "Sin clasificar")."""
        titulos, resumenes = list(titulos), list(resumenes)

        def construir(t: str, r: str):
            usuario = f"TÍTULO:\n{t[:300]}\n\nRESUMEN / CUERPO:\n{r[:TEXTO_MAX_TEMA]}"
            return SISTEMA_TEMA, usuario, "tema_ia", ESQUEMA_TEMA, "tema"
        n_err = len(self.errores)
        temas = await self._lote(titulos, resumenes, construir, on_progress)
        # Las noticias que la API no pudo clasificar reciben un tema por palabras clave.
        nuevos = self.errores[n_err:]
        del self.errores[n_err:]
        for i, tema in enumerate(temas):
            if tema not in TAXONOMIA_TEMAS:
                temas[i] = tema_por_palabras_clave(titulos[i], resumenes[i])
        if nuevos:
            self.avisos.append(
                f"{len(nuevos)} tema(s) asignados por palabras clave porque la API no respondió "
                f"tras {RONDAS_REINTENTO + 1} intentos:\n" + "\n".join(nuevos[:50])
            )
        return temas

    def tono_fijo(self, titulo: str, resumen: str) -> Optional[str]:
        """Tono determinado por reglas (voceros positivos) o None si debe decidirlo el modelo."""
        return "Positivo" if menciona_vocero(titulo, resumen, self.voceros) else None

    async def clasificar_tonos(self, titulos, resumenes, on_progress=None) -> List[str]:
        titulos, resumenes = list(titulos), list(resumenes)
        fijos = {}
        for i in range(len(titulos)):
            tono = self.tono_fijo(titulos[i], resumenes[i])
            if tono:
                fijos[i] = tono
        self.tonos_por_vocero += len(fijos)

        def construir(t: str, r: str):
            usuario = f"TÍTULO:\n{t[:300]}\n\nRESUMEN / CUERPO:\n{r[:TEXTO_MAX_TONO]}"
            return self._sistema_tono, usuario, "tono_ia", ESQUEMA_TONO, "tono"
        return await self._lote(titulos, resumenes, construir, on_progress, fijos)

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
"""
import asyncio
import hashlib
import json
import re
from typing import Callable, Dict, List, Optional, Sequence

from openai import (
    AsyncOpenAI,
    APIConnectionError,
    AuthenticationError,
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

SIN_CLASIFICAR = "Sin clasificar"
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
    "opción por defecto. Si un tercero sufre el hecho grave y la institución solo informa, es NEUTRO.\n\n"
    "EJEMPLOS:\n"
    "- La UNAL entra al top 5 de universidades de Latinoamérica → Positivo\n"
    "- Estudiantes denuncian acoso por parte de un docente de la UNAL → Negativo\n"
    "- Un profesor de la UNAL, citado como fuente, explica cómo se calcula la inflación → Neutro\n"
    "- Paro estudiantil bloquea el ingreso al campus por tercera semana → Negativo\n"
    "- Investigadores de la UNAL desarrollan un sensor que reduce costos de monitoreo del agua → Positivo\n\n"
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
    ):
        self.api_key = api_key
        self.base_url = base_url
        self.marca = marca.strip()
        self.aliases = [a.strip() for a in (aliases or []) if a.strip()]
        self.concurrencia = concurrencia
        # Siempre Luna: un modelo forzado solo se acepta si pertenece a la familia Luna.
        if modelo and "luna" in modelo.lower():
            self.cadena = [modelo] + [m for m in CADENA_MODELOS if m != modelo]
        else:
            self.cadena = list(CADENA_MODELOS)
        self.modelo_activo: Optional[str] = None
        self.tokens_in = 0
        self.tokens_out = 0
        self.errores: List[str] = []
        self._client: Optional[AsyncOpenAI] = None
        self._sin_reasoning_effort = False

        alias_txt = f" (también llamada: {', '.join(self.aliases)})" if self.aliases else ""
        self._sistema_tono = SISTEMA_TONO_TPL.format(marca=self.marca, aliases=alias_txt)

    # ── infraestructura ──────────────────────────────────────────────────────
    def _cliente(self) -> AsyncOpenAI:
        if self._client is None:
            self._client = AsyncOpenAI(
                api_key=self.api_key, base_url=self.base_url, max_retries=4, timeout=90.0
            )
        return self._client

    async def cerrar(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None

    @property
    def costo(self) -> float:
        return costo_usd(self.modelo_activo or self.cadena[0], self.tokens_in, self.tokens_out)

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
                    resp = await self._cliente().chat.completions.create(**kwargs)
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
    ) -> List[str]:
        n = len(titulos)
        claves = [_clave_texto(titulos[i], resumenes[i]) for i in range(n)]
        unicos: Dict[str, int] = {}
        for i, k in enumerate(claves):
            unicos.setdefault(k, i)

        sem = asyncio.Semaphore(self.concurrencia)
        resultados: Dict[str, str] = {}
        hechos = 0
        total = len(unicos)

        async def trabajar(k: str, i: int):
            nonlocal hechos
            async with sem:
                try:
                    resultados[k] = await self._json(*construir(str(titulos[i] or ""), str(resumenes[i] or "")))
                except ErrorClasificador:
                    raise
                except Exception as e:  # error puntual de una noticia: se registra, no se oculta
                    self.errores.append(f"fila {i + 2}: {type(e).__name__}: {e}")
                    resultados[k] = SIN_CLASIFICAR
                hechos += 1
                if on_progress:
                    on_progress(hechos, total)

        await asyncio.gather(*(trabajar(k, i) for k, i in unicos.items()))
        return [resultados[k] for k in claves]

    async def clasificar_temas(self, titulos, resumenes, on_progress=None) -> List[str]:
        def construir(t: str, r: str):
            usuario = f"TÍTULO:\n{t[:300]}\n\nRESUMEN / CUERPO:\n{r[:TEXTO_MAX_TEMA]}"
            return SISTEMA_TEMA, usuario, "tema_ia", ESQUEMA_TEMA, "tema"
        return await self._lote(list(titulos), list(resumenes), construir, on_progress)

    async def clasificar_tonos(self, titulos, resumenes, on_progress=None) -> List[str]:
        def construir(t: str, r: str):
            usuario = f"TÍTULO:\n{t[:300]}\n\nRESUMEN / CUERPO:\n{r[:TEXTO_MAX_TONO]}"
            return self._sistema_tono, usuario, "tono_ia", ESQUEMA_TONO, "tono"
        return await self._lote(list(titulos), list(resumenes), construir, on_progress)

"""Pruebas con un servidor OpenAI simulado (no usa la red ni gasta tokens)."""
import asyncio
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from clasificador import (  # noqa: E402
    NOMBRES_TEMAS, SIN_CLASIFICAR, ClasificadorIA, ErrorClasificador,
    _espera_sugerida, menciona_vocero, tema_por_palabras_clave,
)
from textos import normalize_title_for_comparison  # noqa: E402

REGLAS = [
    ("bloque", "Conflicto, orden público y seguridad", "Negativo"),
    ("ranking", "Acreditación y calidad educativa", "Positivo"),
    ("beca", "Bienestar universitario", "Neutro"),
    ("presupuesto", "Financiamiento y presupuesto", "Negativo"),
]


class FakeOpenAI(BaseHTTPRequestHandler):
    config = {}
    peticiones = []

    def log_message(self, *a):
        pass

    def _send(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        cfg = FakeOpenAI.config
        FakeOpenAI.peticiones.append(req)
        modelo = req["model"]
        if cfg.get("rate_limit", 0) > 0:
            cfg["rate_limit"] -= 1
            return self._send(429, {"error": {"message": f"Rate limit reached for {modelo} on tokens per min (TPM): Limit 2000000, Used 2000000, Requested 3074. Please try again in 92ms.", "type": "tokens", "code": "rate_limit_exceeded"}})
        if cfg.get("sin_cuota"):
            return self._send(429, {"error": {"message": "You exceeded your current quota", "type": "insufficient_quota", "code": "insufficient_quota"}})
        if cfg.get("auth_error"):
            return self._send(401, {"error": {"message": "bad key", "type": "invalid_request_error", "code": "invalid_api_key"}})
        if modelo in cfg.get("retirados", ()):
            return self._send(404, {"error": {"message": f"The model `{modelo}` does not exist", "type": "invalid_request_error", "code": "model_not_found"}})
        if cfg.get("rechaza_reasoning") and "reasoning_effort" in req:
            return self._send(400, {"error": {"message": "Unsupported parameter", "type": "invalid_request_error", "param": "reasoning_effort", "code": "unsupported_parameter"}})
        assert "temperature" not in req and "max_tokens" not in req
        assert "max_completion_tokens" in req and req["response_format"]["type"] == "json_schema"
        usuario = req["messages"][-1]["content"].lower()
        props = req["response_format"]["json_schema"]["schema"]["properties"]
        campo = "tema" if "tema" in props else "tono"
        valor = None
        for kw, tema, tono in REGLAS:
            if kw in usuario:
                valor = tema if campo == "tema" else tono
                break
        if valor is None:
            valor = "Vida universitaria" if campo == "tema" else "Neutro"
        if "valor_invalido" in usuario:
            valor = "Categoría inventada"
        self._send(200, {
            "id": "x", "object": "chat.completion", "created": 0, "model": modelo,
            "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": json.dumps({"evidencia": "ok", campo: valor})}}],
            "usage": {"prompt_tokens": 1000, "completion_tokens": 20, "total_tokens": 1020},
        })


@pytest.fixture()
def servidor():
    srv = HTTPServer(("127.0.0.1", 0), FakeOpenAI)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    FakeOpenAI.config = {}
    FakeOpenAI.peticiones = []
    yield f"http://127.0.0.1:{srv.server_address[1]}/v1"
    srv.shutdown()


def nuevo(url, **kw):
    return ClasificadorIA("sk-test", "Universidad Nacional", ["UNAL"], base_url=url, espera_base=0.01, **kw)


def correr(coro):
    return asyncio.run(coro)


def test_clasifica_cada_noticia_con_su_propio_texto(servidor):
    titulos = ["Universidad Nacional: estudiantes bloquean la entrada",
               "Universidad Nacional: sube en el ranking QS",
               "Universidad Nacional: abre convocatoria de becas",
               "Universidad Nacional: foro sobre arte"]
    res = ["x"] * 4

    async def go():
        c = nuevo(servidor)
        try:
            return await c.clasificar_temas(titulos, res), await c.clasificar_tonos(titulos, res), c
        finally:
            await c.cerrar()

    temas, tonos, c = correr(go())
    assert temas == ["Conflicto, orden público y seguridad", "Acreditación y calidad educativa",
                     "Bienestar universitario", "Vida universitaria"]
    assert tonos == ["Negativo", "Positivo", "Neutro", "Neutro"]
    assert all(t in NOMBRES_TEMAS for t in temas)
    assert c.modelo_activo == "gpt-6-luna" and c.tokens_in == 8000 and not c.errores


def test_textos_identicos_se_llaman_una_sola_vez(servidor):
    async def go():
        c = nuevo(servidor)
        try:
            return await c.clasificar_temas(["Ranking QS"] * 5, ["mismo texto"] * 5)
        finally:
            await c.cerrar()
    assert correr(go()) == ["Acreditación y calidad educativa"] * 5
    assert len(FakeOpenAI.peticiones) == 1


def test_modelo_retirado_usa_respaldo(servidor):
    FakeOpenAI.config = {"retirados": {"gpt-6-luna"}}

    async def go():
        c = nuevo(servidor)
        try:
            return await c.verificar(), c
        finally:
            await c.cerrar()
    modelo, c = correr(go())
    assert modelo == "gpt-5.6-luna" and c.modelo_activo == "gpt-5.6-luna"


def test_sin_modelos_disponibles_falla_ruidosamente(servidor):
    FakeOpenAI.config = {"retirados": {"gpt-6-luna", "gpt-5.6-luna"}}

    async def go():
        c = nuevo(servidor)
        try:
            await c.verificar()
        finally:
            await c.cerrar()
    with pytest.raises(ErrorClasificador, match="Ningún modelo disponible"):
        correr(go())


def test_modelo_retirado_antiguo_no_se_oculta(servidor):
    """El bug original: gpt-4.1-nano retirado => todo 'Neutro'. Ahora debe fallar con error claro."""
    FakeOpenAI.config = {"retirados": {"gpt-4.1-nano-2025-04-14", "gpt-6-luna", "gpt-5.6-luna"}}

    async def go():
        c = ClasificadorIA("sk", "UNAL", modelo="gpt-4.1-nano-2025-04-14", base_url=servidor)
        try:
            await c.clasificar_tonos(["t"], ["r"])
        finally:
            await c.cerrar()
    with pytest.raises(ErrorClasificador):
        correr(go())


def test_clave_invalida(servidor):
    FakeOpenAI.config = {"auth_error": True}

    async def go():
        c = nuevo(servidor)
        try:
            await c.verificar()
        finally:
            await c.cerrar()
    with pytest.raises(ErrorClasificador, match="OPENAI_API_KEY"):
        correr(go())


def test_reasoning_effort_rechazado_se_reintenta_sin_el(servidor):
    FakeOpenAI.config = {"rechaza_reasoning": True}

    async def go():
        c = nuevo(servidor)
        try:
            return await c.clasificar_tonos(["ranking"], ["x"])
        finally:
            await c.cerrar()
    assert correr(go()) == ["Positivo"]
    assert "reasoning_effort" not in FakeOpenAI.peticiones[-1]


def test_error_por_fila_en_tono_queda_registrado_no_disfrazado(servidor):
    async def go():
        c = nuevo(servidor)
        try:
            return await c.clasificar_tonos(["valor_invalido", "ranking"], ["a", "b"]), c
        finally:
            await c.cerrar()
    tonos, c = correr(go())
    assert tonos == [SIN_CLASIFICAR, "Positivo"]
    assert len(c.errores) == 1 and "fuera de catálogo" in c.errores[0]


def test_tema_nunca_queda_sin_clasificar(servidor):
    async def go():
        c = nuevo(servidor)
        try:
            return await c.clasificar_temas(
                ["valor_invalido: estudiantes bloquean la 26", "ranking"], ["a", "b"]), c
        finally:
            await c.cerrar()
    temas, c = correr(go())
    assert SIN_CLASIFICAR not in temas and all(t in NOMBRES_TEMAS for t in temas)
    assert temas[0] == "Conflicto, orden público y seguridad"   # respaldo por palabras clave
    assert not c.errores and len(c.avisos) == 1 and "palabras clave" in c.avisos[0]


def test_429_por_tpm_se_reintenta_y_no_pierde_noticias(servidor):
    FakeOpenAI.config = {"rate_limit": 12}
    titulos = [f"ranking {i}" for i in range(10)]

    async def go():
        c = nuevo(servidor, concurrencia=10)
        try:
            return await c.clasificar_tonos(titulos, ["x"] * 10), c
        finally:
            await c.cerrar()
    tonos, c = correr(go())
    assert tonos == ["Positivo"] * 10
    assert not c.errores and c.reintentos_429 >= 1


def test_429_persistente_se_recupera_en_rondas_finales(servidor):
    # Más 429 que los reintentos de una sola petición: las rondas finales la recuperan.
    FakeOpenAI.config = {"rate_limit": 25}

    async def go():
        c = nuevo(servidor, concurrencia=1)
        try:
            return await c.clasificar_temas(["ranking"], ["x"]), c
        finally:
            await c.cerrar()
    temas, c = correr(go())
    assert temas == ["Acreditación y calidad educativa"] and not c.avisos


def test_sin_cuota_falla_ruidosamente(servidor):
    FakeOpenAI.config = {"sin_cuota": True}

    async def go():
        c = nuevo(servidor)
        try:
            await c.clasificar_tonos(["t"], ["r"])
        finally:
            await c.cerrar()
    with pytest.raises(ErrorClasificador, match="saldo"):
        correr(go())


def test_espera_sugerida():
    assert abs(_espera_sugerida(Exception("Please try again in 92ms. Visit")) - 0.092) < 1e-9
    assert _espera_sugerida(Exception("try again in 1m3.5s")) == 63.5
    assert _espera_sugerida(Exception("otro error")) == 0.0


def test_vocero_rector_siempre_positivo(servidor):
    texto = ("José Ismael Peña, rector de la Universidad Nacional, señaló que la institución enfrenta "
             "el reto de la reducción del número de bachilleres en Colombia. Según explicó, el país pasó "
             "de cerca de 900.000 graduados hace 15 años a aproximadamente 450.000 en la actualidad.")

    async def go():
        c = nuevo(servidor)
        try:
            # "bloque" haría que el modelo simulado responda Negativo: el vocero manda.
            return await c.clasificar_tonos(["Rector habla de bloqueos", "bloqueo en la 26"], [texto, "x"]), c
        finally:
            await c.cerrar()
    tonos, c = correr(go())
    assert tonos == ["Positivo", "Negativo"] and c.tonos_por_vocero == 1
    assert len(FakeOpenAI.peticiones) == 1   # la noticia del vocero no gasta tokens
    assert "José Ismael Peña" in c._sistema_tono


def test_menciona_vocero_variantes():
    v = ["José Ismael Peña"]
    assert menciona_vocero("", "JOSE ISMAEL PEÑA dijo", v)
    assert menciona_vocero("El rector Peña habló", "", v)
    assert menciona_vocero("Ismael Peña, rector", "", v)
    assert not menciona_vocero("Enrique Peñalosa visitó la UNAL", "", v)
    assert not menciona_vocero("José Ismael Peña", "", [])


def test_tema_por_palabras_clave_siempre_en_taxonomia():
    assert tema_por_palabras_clave("", "") in NOMBRES_TEMAS
    assert tema_por_palabras_clave("UNAL sube en el ranking QS", "") == "Acreditación y calidad educativa"


def test_normalizador_de_titulos_no_colapsa_prefijos():
    a = normalize_title_for_comparison("Universidad Nacional: abre convocatoria de becas")
    b = normalize_title_for_comparison("Universidad Nacional: estudiantes bloquean la entrada por paro")
    assert a != b
    assert normalize_title_for_comparison("Covid-19 en la UNAL") != normalize_title_for_comparison("Covid-20 en la UNAL")
    # el sufijo corto del medio sí se ignora
    assert normalize_title_for_comparison("Paro en la UNAL | El Tiempo") == normalize_title_for_comparison("Paro en la UNAL")
    assert normalize_title_for_comparison(None) == ""


def test_siempre_luna(servidor):
    assert ClasificadorIA("k", "m", modelo="gpt-4.1-nano").cadena == ["gpt-6-luna", "gpt-5.6-luna"]
    assert ClasificadorIA("k", "m", modelo="gpt-5.6-luna").cadena[0] == "gpt-5.6-luna"

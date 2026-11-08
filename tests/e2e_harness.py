"""Prueba de punta a punta de run_full_process_async con un servidor OpenAI simulado.
Uso:  python tests/e2e_harness.py   (lo ejecuta tests/test_e2e.py)"""
import asyncio, io, os, sys, threading
from http.server import HTTPServer
from pathlib import Path
import warnings, logging
warnings.filterwarnings("ignore"); logging.disable(logging.CRITICAL)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT)); sys.path.insert(0, str(ROOT / "tests"))
os.chdir(ROOT)

from test_clasificador import FakeOpenAI  # noqa: E402

retirados = set(filter(None, os.environ.get("RETIRADOS", "").split(",")))
FakeOpenAI.config = {"retirados": retirados, "rate_limit": int(os.environ.get("RATE_LIMIT", "0"))}
srv = HTTPServer(("127.0.0.1", 0), FakeOpenAI)
threading.Thread(target=srv.serve_forever, daemon=True).start()
os.environ["OPENAI_BASE_URL"] = f"http://127.0.0.1:{srv.server_address[1]}/v1"

import tempfile  # noqa: E402
_home = tempfile.mkdtemp()
os.makedirs(os.path.join(_home, ".streamlit"))
open(os.path.join(_home, ".streamlit", "secrets.toml"), "w").write('OPENAI_API_KEY = "sk-test"\nAPP_PASSWORD = "x"\n')
os.environ["HOME"] = _home   # streamlit lee ~/.streamlit/secrets.toml

import streamlit as st  # noqa: E402
import openpyxl  # noqa: E402

class Stop(Exception):
    pass
def _stop():
    raise Stop()
st.stop = _stop

import app  # noqa: E402

# ── dossier de ejemplo ───────────────────────────────────────────────────────
wb = openpyxl.Workbook(); ws = wb.active
ws.append(["NoticiaId", "Tipo de Medio", "Medio", "Fecha", "Hora", "Título", "Empresa rel.", "CuerpoEs",
           "Tono", "Tematica", "URL Nota", "Valor de Nota"])
U = "Universidad Nacional de Colombia - General"
filas = [
    (1, "Online", "El Tiempo", "03/11/2026", "10:00", "Universidad Nacional: estudiantes bloquean la entrada", U, "Bloqueo en la calle 26", "", "", "http://a.com/1", 100),
    (2, "Online", "Semana", "03/11/2026", "11:00", "Universidad Nacional: sube en el ranking QS", U, "Ranking QS 2027 favorable", "", "", "http://a.com/2", 100),
    (3, "Online", "El Espectador", "03/11/2026", "12:00", "Universidad Nacional: abre convocatoria de becas", U, "Becas para estudiantes", "", "", "http://a.com/3", 100),
    (4, "Online", "Portafolio", "03/11/2026", "13:00", "Otra empresa lanza producto", "Bancolombia", "texto", "", "", "http://a.com/4", 100),
    (5, "Online", "El Colombiano", "03/11/2026", "14:00", "Universidad Nacional: recorte del presupuesto", U, "Presupuesto en déficit", "", "", "http://a.com/5", 100),
    (6, "Online", "La República", "03/11/2026", "15:00", "Rector advierte caída de bachilleres y bloqueo del sistema", U,
     "José Ismael Peña, rector de la Universidad Nacional, señaló que la institución enfrenta el reto de la reducción del número de bachilleres en Colombia.", "", "", "http://a.com/6", 100),
]
for f in filas: ws.append(f)
buf = io.BytesIO(); wb.save(buf); buf.seek(0)

async def main():
    try:
        await app.run_full_process_async(buf, "Universidad Nacional", ["UNAL"], None, None, "API de OpenAI")
    except Stop:
        print("STOP: la app se detuvo con error"); return 2
    out = openpyxl.load_workbook(io.BytesIO(st.session_state["output_data"]))
    hoja = out["UNAL"]
    cab = [c.value for c in hoja[1]]
    it, itn, itm = cab.index("Título"), cab.index("Tono IA"), cab.index("Tema IA")
    for r in hoja.iter_rows(min_row=2, values_only=True):
        print(f"{r[it][:45]:45} | {r[itn]:10} | {r[itm]}")
    sin_tema = []
    for h in out.worksheets:
        cab_h = [c.value for c in h[1]]
        if "Tema IA" not in cab_h:
            continue
        j = cab_h.index("Tema IA")
        sin_tema += [(h.title, r) for r in h.iter_rows(min_row=2, values_only=True) if r[j] in (None, "", app.SIN_CLASIFICAR)]
    print("SIN TEMA:", len(sin_tema), "| hojas revisadas:", [h.title for h in out.worksheets if "Tema IA" in [c.value for c in h[1]]])
    print("COSTO:", st.session_state["process_cost"], "|", st.session_state["cache_stats"])
    return 0

sys.exit(asyncio.run(main()))

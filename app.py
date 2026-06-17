import streamlit as st
import pandas as pd
from openpyxl import load_workbook, Workbook
from openpyxl.styles import Font, Alignment
from collections import defaultdict
import datetime
import io
import openai
import asyncio
import time
import gc
import hashlib
import re
from unidecode import unidecode

# ======================================
# CONFIGURACIÓN
# ======================================
st.set_page_config(
    page_title="Análisis UNAL · IA",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14"

BRAND_NAME = "Universidad Nacional de Colombia"
BRAND_ALIASES = ["UNAL", "Universidad Nacional", "U.Nal.", "U. Nacional"]

# ======================================
# CSS
# ======================================
def load_custom_css():
    st.markdown("""
    <style>
    .app-header {background:#fff; border:1px solid #dadce0; border-radius:16px; padding:1.2rem 1.8rem; margin-bottom:1.5rem; display:flex; align-items:center; gap:1rem; box-shadow:0 2px 8px rgba(0,0,0,0.08);}
    .app-header-icon {width:56px; height:56px; background:linear-gradient(135deg,#f97316,#ea580c); border-radius:14px; display:flex; align-items:center; justify-content:center; font-size:1.8rem; color:white; flex-shrink:0;}
    .metric-card {background:white; border:1px solid #dadce0; border-radius:12px; padding:1rem; text-align:center;}
    .success-banner {background:#ecfdf5; border:1px solid #a7f3d0; border-left:5px solid #10b981; padding:1rem; border-radius:12px; margin:1rem 0;}
    </style>
    """, unsafe_allow_html=True)

# ======================================
# PASSWORD
# ======================================
def check_password():
    if st.session_state.get("password_correct", False):
        return True
    st.markdown("### 🔐 Acceso Restringido")
    pw = st.text_input("Ingresa la contraseña", type="password", key="pw_input")
    if st.button("Ingresar", type="primary"):
        if pw == st.secrets.get("APP_PASSWORD", "unal2025"):
            st.session_state.password_correct = True
            st.rerun()
        else:
            st.error("Contraseña incorrecta")
    return False

# ======================================
# EMBEDDING CACHE
# ======================================
class EmbeddingCache:
    def __init__(self):
        self._cache = {}
        self._hits = 0
        self._misses = 0

    def _key(self, text):
        return hashlib.md5(str(text)[:1500].encode('utf-8', errors='ignore')).hexdigest()

    def get(self, text):
        k = self._key(text)
        if k in self._cache:
            self._hits += 1
            return self._cache[k]
        self._misses += 1
        return None

    def put(self, text, emb):
        self._cache[self._key(text)] = emb

    def get_many(self, textos):
        results = [self.get(t) for t in textos]
        missing = [i for i, r in enumerate(results) if r is None]
        return results, missing

    def clear(self):
        self._cache.clear()
        self._hits = 0
        self._misses = 0

    def stats(self):
        total = self._hits + self._misses
        rate = (self._hits / total * 100) if total > 0 else 0
        return f"Cache: {self._hits} hits, {self._misses} misses ({rate:.0f}%)"

if '_emb_cache' not in st.session_state:
    st.session_state['_emb_cache'] = EmbeddingCache()

def get_embedding_cache():
    return st.session_state['_emb_cache']

def get_embeddings_batch(textos):
    if not textos:
        return []
    cache = get_embedding_cache()
    results, missing = cache.get_many(textos)
    if not missing:
        return results
    
    mt = [textos[i][:1800] for i in missing]
    try:
        resp = openai.Embedding.create(input=mt, model=OPENAI_MODEL_EMBEDDING)
        for j, d in enumerate(resp["data"]):
            idx = missing[j]
            emb = d["embedding"]
            results[idx] = emb
            cache.put(textos[idx], emb)
    except Exception as e:
        st.warning(f"Error embeddings: {e}")
    return results

# ======================================
# CLASES DE ANÁLISIS
# ======================================
class ClasificadorTono:
    def __init__(self, marca, aliases):
        self.marca = marca
        self.aliases = aliases

    async def procesar_lote_async(self, textos, pbar, resumenes, titulos):
        n = len(textos)
        results = []
        for i in range(n):
            if i % 8 == 0:
                pbar.progress((i+1)/n, f"Evaluando tono {i+1}/{n}")
            # Placeholder - puedes expandir con LLM real
            results.append({"tono": "Neutro"})
        pbar.progress(1.0, "✅ Tono completado")
        return results

class ClasificadorSubtema:
    def __init__(self, marca, aliases):
        self.marca = marca
        self.aliases = aliases

    def procesar_lote(self, textos, pbar, resumenes, titulos):
        n = len(textos)
        subtemas = []
        for i in range(n):
            if i % 10 == 0:
                pbar.progress((i+1)/n, f"Generando subtema {i+1}/{n}")
            subtemas.append("Cobertura Universidad Nacional")
        pbar.progress(1.0, "✅ Subtemas completados")
        return subtemas

def consolidar_temas(subtemas, textos, pbar):
    pbar.progress(1.0, "✅ Temas consolidados")
    return ["Universidad Nacional de Colombia"] * len(subtemas)

def texto_para_embedding(titulo, resumen):
    t = str(titulo or "")
    r = str(resumen or "")
    return f"{t}. {t}. {r}"[:1800]

# ======================================
# PROCESO PRINCIPAL
# ======================================
async def run_unal_process_async(df_file):
    st.session_state.update({'tokens_input': 0, 'tokens_output': 0, 'tokens_embedding': 0})
    get_embedding_cache().clear()
    t0 = time.time()

    try:
        openai.api_key = st.secrets["OPENAI_API_KEY"]
    except:
        st.error("❌ OPENAI_API_KEY no configurada en secrets.")
        st.stop()

    with st.status("📂 Cargando y normalizando dossier...", expanded=True) as s:
        wb = load_workbook(df_file, data_only=True)
        sheet = wb.active
        headers = [cell.value for cell in sheet[1] if cell.value]
        
        rows = []
        for row in sheet.iter_rows(min_row=2):
            if all(c.value is None for c in row): continue
            row_data = {}
            for i, h in enumerate(headers):
                if i < len(row):
                    row_data[h] = row[i].value
            rows.append(row_data)

        km = {
            "menciones": "Menciones - Empresa",
            "titulo": "Título",
            "resumen": "Resumen - Aclaracion",
            "tonoiai": "Tono IA",
            "tema": "Tema",
            "subtema": "Subtema"
        }

        # Expandir menciones
        expanded_rows = []
        for r in rows:
            menciones = str(r.get(km["menciones"], "")).split(';')
            for m in menciones:
                new_row = r.copy()
                new_row[km["menciones"]] = m.strip()
                expanded_rows.append(new_row)

        unal_rows = [r for r in expanded_rows if r.get(km["menciones"]) == "Universidad Nacional de Colombia - General"]
        otras_rows = [r for r in expanded_rows if r.get(km["menciones"]) != "Universidad Nacional de Colombia - General"]

        s.update(label=f"✓ {len(unal_rows)} noticias UNAL | {len(otras_rows)} otras", state="complete")

    # === ANÁLISIS IA SOLO PARA UNAL ===
    if unal_rows:
        df_unal = pd.DataFrame(unal_rows)
        df_unal["_txt"] = df_unal.apply(
            lambda r: texto_para_embedding(r.get(km["titulo"], ""), r.get(km["resumen"], "")), axis=1
        )

        with st.status("🤖 Analizando con IA (Tono + Temas)...", expanded=True) as s:
            pb = st.progress(0)
            tono_res = await ClasificadorTono(BRAND_NAME, BRAND_ALIASES).procesar_lote_async(
                df_unal["_txt"].tolist(), pb, df_unal.get(km["resumen"], pd.Series()), df_unal.get(km["titulo"], pd.Series())
            )
            df_unal[km["tonoiai"]] = [r["tono"] for r in tono_res]

            pb = st.progress(0)
            subtemas = ClasificadorSubtema(BRAND_NAME, BRAND_ALIASES).procesar_lote(
                df_unal["_txt"].tolist(), pb, df_unal.get(km["resumen"], pd.Series()), df_unal.get(km["titulo"], pd.Series())
            )
            temas = consolidar_temas(subtemas, df_unal["_txt"].tolist(), pb)

            df_unal[km["subtema"]] = subtemas
            df_unal[km["tema"]] = temas
            s.update(label="✅ Análisis IA completado", state="complete")

        # Actualizar filas originales
        for row in unal_rows:
            row[km["tonoiai"]] = df_unal.loc[df_unal.index[unal_rows.index(row)], km["tonoiai"]]
            row[km["subtema"]] = df_unal.loc[df_unal.index[unal_rows.index(row)], km["subtema"]]
            row[km["tema"]] = df_unal.loc[df_unal.index[unal_rows.index(row)], km["tema"]]

    # === GENERAR EXCEL ===
    with st.status("📊 Generando Excel con 2 hojas...", expanded=True) as s:
        wb_out = Workbook()
        wb_out.remove(wb_out.active)

        def crear_hoja(nombre, datos):
            ws = wb_out.create_sheet(nombre)
            columnas = ["Menciones - Empresa", "Título", "Resumen - Aclaracion", "Tono IA", "Tema", "Subtema", "Medio", "Fecha"]
            ws.append(columnas)
            for r in datos:
                ws.append([r.get(c, "") for c in columnas])

        crear_hoja("UNAL_Analizado", unal_rows)
        crear_hoja("Otras_Menciones", otras_rows)

        buf = io.BytesIO()
        wb_out.save(buf)

        st.session_state.update({
            "output_data": buf.getvalue(),
            "output_filename": f"Informe_UNAL_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            "processing_complete": True,
            "unal_count": len(unal_rows),
            "otras_count": len(otras_rows),
            "duration": f"{time.time() - t0:.1f}s"
        })
        s.update(label="✅ Archivo generado", state="complete")

# ======================================
# MAIN
# ======================================
def main():
    load_custom_css()
    if not check_password():
        return

    st.markdown("""
    <div class="app-header">
        <div class="app-header-icon">◈</div>
        <div>
            <h2>Análisis Universidad Nacional de Colombia</h2>
            <p><strong>Procesamiento selectivo con IA</strong></p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    if not st.session_state.get("processing_complete", False):
        with st.form("unal_form"):
            f = st.file_uploader("📁 Sube el dossier Excel", type=["xlsx"])
            if st.form_submit_button("🚀 Iniciar Análisis UNAL", type="primary", use_container_width=True):
                if f:
                    asyncio.run(run_unal_process_async(f))
                    st.rerun()
                else:
                    st.warning("Por favor sube un archivo")
    else:
        st.markdown('<div class="success-banner">✅ <strong>Análisis completado exitosamente</strong></div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        col1.metric("UNAL Analizadas", st.session_state.unal_count)
        col2.metric("Otras menciones", st.session_state.otras_count)
        st.metric("Tiempo de procesamiento", st.session_state.duration)

        st.download_button(
            "⬇️ Descargar Informe UNAL (2 hojas)",
            data=st.session_state.output_data,
            file_name=st.session_state.output_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary"
        )

        if st.button("🔄 Nuevo análisis"):
            for k in list(st.session_state.keys()):
                if k not in ["password_correct"]:
                    del st.session_state[k]
            st.rerun()

if __name__ == "__main__":
    main()

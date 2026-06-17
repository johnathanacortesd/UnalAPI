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

# ==================== CONFIGURACIÓN ====================
st.set_page_config(
    page_title="Análisis UNAL · IA",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

OPENAI_MODEL_EMBEDDING = "text-embedding-3-small"
OPENAI_MODEL_CLASIFICACION = "gpt-4.1-nano-2025-04-14"

# Marca fija
BRAND_NAME = "Universidad Nacional de Colombia"
BRAND_ALIASES = ["UNAL", "Universidad Nacional", "U.Nal."]

# =====================================================

# (Mantengo todas tus funciones de limpieza, CSS, EmbeddingCache, etc.)
# Copia aquí todo el código que ya tienes desde el principio hasta antes de run_full_process_async

# ... [Pega aquí todo tu código original hasta la función run_full_process_async] ...

# ==================== NUEVA FUNCIÓN PRINCIPAL ====================

async def run_unal_process_async(df_file):
    st.session_state.update({'tokens_input': 0, 'tokens_output': 0, 'tokens_embedding': 0})
    get_embedding_cache().clear()
    t0 = time.time()

    try:
        openai.api_key = st.secrets["OPENAI_API_KEY"]
    except:
        st.error("OPENAI_API_KEY no encontrada en secrets.")
        st.stop()

    with st.status("Cargando y normalizando dossier...", expanded=True) as s:
        config_path = load_local_config()
        if not config_path:
            st.error("No se encontró Configuracion.xlsx")
            st.stop()

        region_map, internet_map = load_config(config_path)
        wb_in = load_workbook(df_file, data_only=True)
        df_normalized = read_and_normalize_dossier(wb_in.active, region_map, internet_map)

        # Expansión por menciones
        rows_expanded = []
        for idx, row_series in df_normalized.iterrows():
            menciones = [m.strip() for m in str(row_series.get('Menciones - Empresa', '')).split(';') if m.strip()]
            if not menciones:
                menciones = [""]
            for m in menciones:
                row_dict = row_series.to_dict()
                row_dict['Menciones - Empresa'] = m
                row_dict['original_index'] = idx
                row_dict['is_duplicate'] = False
                rows_expanded.append(row_dict)

        km = {  # key mapping
            "idnoticia": "ID Noticia", "fecha": "Fecha", "hora": "Hora",
            "medio": "Medio", "tipodemedio": "Tipo de Medio",
            "seccion_programa": "Sección - Programa", "region": "Región",
            "titulo": "Título", "resumen": "Resumen - Aclaracion",
            "tono": "Tono", "tonoiai": "Tono IA", "tema": "Tema", "subtema": "Subtema",
            "link_nota": "Link Nota", "link_streaming": "Link (Streaming - Imagen)",
            "menciones": "Menciones - Empresa", "idduplicada": "ID duplicada"
        }

        rows = detectar_duplicados_avanzado(rows_expanded, km)

        # Separar UNAL y Otras
        unal_rows = [r for r in rows if str(r.get(km["menciones"], "")).strip() == "Universidad Nacional de Colombia - General"]
        otras_rows = [r for r in rows if str(r.get(km["menciones"], "")).strip() != "Universidad Nacional de Colombia - General"]

        s.update(label="✓ Carga completada", state="complete")

    # =============== ANÁLISIS SOLO PARA UNAL ===============
    if unal_rows:
        df_unal = pd.DataFrame(unal_rows)
        df_unal["_txt"] = df_unal.apply(
            lambda r: texto_para_embedding(str(r.get(km["titulo"], "")), str(r.get(km["resumen"], ""))), axis=1
        )

        with st.status("Analizando Tono y Temas para UNAL...", expanded=True) as s:
            pb = st.progress(0)

            # Tono
            tono_results = await ClasificadorTono(BRAND_NAME, BRAND_ALIASES).procesar_lote_async(
                df_unal["_txt"], pb, df_unal[km["resumen"]], df_unal[km["titulo"]]
            )
            df_unal[km["tonoiai"]] = [r["tono"] for r in tono_results]

            # Subtemas y Temas
            pb = st.progress(0)
            subtemas = ClasificadorSubtema(BRAND_NAME, BRAND_ALIASES).procesar_lote(
                df_unal["_txt"], pb, df_unal[km["resumen"]], df_unal[km["titulo"]]
            )
            temas = consolidar_temas(subtemas, df_unal["_txt"].tolist(), pb)

            df_unal[km["subtema"]] = subtemas
            df_unal[km["tema"]] = temas

            s.update(label="✓ Análisis IA completado", state="complete")

        # Actualizar filas originales
        unal_dict = df_unal.set_index("original_index").to_dict("index")
        for row in unal_rows:
            orig_idx = row.get("original_index")
            if orig_idx in unal_dict:
                row.update(unal_dict[orig_idx])

    # =============== GENERAR EXCEL CON DOS HOJAS ===============
    with st.status("Generando Excel...", expanded=True) as s:
        wb = Workbook()
        wb.remove(wb.active)

        # Hoja 1: UNAL
        ws_unal = wb.create_sheet("UNAL_Analizado")
        generar_hoja(ws_unal, unal_rows, km)

        # Hoja 2: Otras menciones
        ws_otras = wb.create_sheet("Otras_Menciones")
        generar_hoja(ws_otras, otras_rows, km)

        buf = io.BytesIO()
        wb.save(buf)
        output_data = buf.getvalue()

        filename = f"Informe_UNAL_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

        st.session_state.update({
            "output_data": output_data,
            "output_filename": filename,
            "processing_complete": True,
            "unal_count": len(unal_rows),
            "otras_count": len(otras_rows),
            "process_duration": f"{time.time() - t0:.0f}s"
        })
        s.update(label="✓ Excel generado", state="complete")


def generar_hoja(ws, rows, km):
    ORDER = [
        "ID Noticia", "Fecha", "Hora", "Medio", "Tipo de Medio", "Sección - Programa",
        "Región", "Título", "Autor - Conductor", "Nro. Pagina", "Dimensión",
        "Duración - Nro. Caracteres", "CPE", "Tier", "Audiencia",
        "Tono", "Tono IA", "Tema", "Subtema", "Link Nota",
        "Resumen - Aclaracion", "Link (Streaming - Imagen)", "Menciones - Empresa", "ID duplicada"
    ]
    ws.append(ORDER)

    font_header = Font(bold=True)
    font_link = Font(color="0563C1", underline="single")

    for i, col_name in enumerate(ORDER, 1):
        cell = ws.cell(row=1, column=i)
        cell.font = font_header

    for row in rows:
        out = []
        links = {}
        for h in ORDER:
            val = row.get(h)
            if isinstance(val, dict) and "url" in val:
                out.append(val.get("value", "Link"))
                if val.get("url"):
                    links[len(out)] = val["url"]
            else:
                out.append(str(val) if val is not None else "")
        ws.append(out)

        current_row = ws.max_row
        for col_idx, url in links.items():
            cell = ws.cell(row=current_row, column=col_idx)
            cell.hyperlink = url
            cell.font = font_link


# ==================== INTERFAZ ====================
def main():
    load_custom_css()
    if not check_password(): return

    st.markdown("""
    <div class="app-header">
        <div class="app-header-icon">◈</div>
        <div class="app-header-text">
            <div class="app-header-title">Análisis UNAL - IA</div>
            <div class="app-header-version">vUNAL · Universidad Nacional de Colombia</div>
        </div>
    </div>""", unsafe_allow_html=True)

    if not st.session_state.get("processing_complete", False):
        with st.form("unal_form"):
            st.markdown("### Sube el dossier")
            f1 = st.file_uploader("Archivo Excel", type=["xlsx"], key="f1")
            if st.form_submit_button("🚀 Iniciar Análisis UNAL", use_container_width=True, type="primary"):
                if f1:
                    asyncio.run(run_unal_process_async(f1))
                    st.rerun()
                else:
                    st.error("Sube un archivo")
    else:
        st.success("✅ Análisis completado")
        st.metric("Noticias UNAL analizadas", st.session_state.get("unal_count", 0))
        st.metric("Otras menciones", st.session_state.get("otras_count", 0))
        st.metric("Duración", st.session_state.get("process_duration", ""))

        st.download_button(
            "⬇ Descargar Informe UNAL (2 hojas)",
            data=st.session_state.output_data,
            file_name=st.session_state.output_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary"
        )

        if st.button("Nuevo análisis"):
            for k in list(st.session_state.keys()):
                if k != "password_correct":
                    del st.session_state[k]
            st.rerun()


if __name__ == "__main__":
    main()

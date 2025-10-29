# ======================================
# Importaciones
# ======================================
import streamlit as st
import pandas as pd
from openpyxl import load_workbook, Workbook
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
import json
import asyncio
from typing import List, Dict, Tuple

# ### NUEVO: Importaciones para modelos locales ###
from transformers import AutoTokenizer, AutoModelForSequenceClassification, AutoModel
from sentence_transformers import SentenceTransformer
import torch
from sklearn.cluster import KMeans
# ======================================
# Configuracion general
# ======================================
st.set_page_config(
    page_title="Análisis de Noticias para la Universidad Nacional",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ### MODIFICADO: Se mantienen solo modelos de OpenAI para etiquetado ###
OPENAI_MODEL_ETIQUETADO = "gpt-4.1-nano-2025-04-14"

# Marcas objetivo a analizar
TARGET_BRANDS = ["U. Nacional de Colombia", "Universidad Nacional de Colombia"]

# Parámetros
SIMILARITY_THRESHOLD_TITULOS = 0.95
MAX_TOKENS_PROMPT_TXT = 4000
NUM_TEMAS_CLUSTERING = 20 # <<< Número de temas a generar

# ======================================
# Estilos CSS (Personalizados para la UNAL)  <--- PEGA EL BLOQUE AQUÍ
# ======================================
def load_custom_css():
    st.markdown(
        """
        <style>
        :root { --primary-color: #005A3A; --secondary-color: #B38612; --card-bg: #ffffff; --shadow-light: 0 2px 4px rgba(0,0,0,0.1); --border-radius: 12px; }
        .main-header { background: linear-gradient(135deg, var(--primary-color) 0%, var(--secondary-color) 100%); color: white; padding: 2rem; border-radius: var(--border-radius); text-align: center; font-size: 2.2rem; font-weight: 800; margin-bottom: 1.5rem; box-shadow: var(--shadow-light); }
        .stButton > button { border-radius: 8px; font-weight: 600; }
        </style>
        """,
        unsafe_allow_html=True,
    )

# ======================================
# Autenticacion y Utilidades
# ======================================
def check_password() -> bool:
# ======================================
# ### NUEVO: Carga de Modelos Locales con Caché ###
# ======================================
@st.cache_resource
def cargar_modelo_sentimiento():
    """Carga el modelo y tokenizador de sentimiento de Hugging Face."""
    tokenizer = AutoTokenizer.from_pretrained("clapAI/roberta-large-multilingual-sentiment")
    model = AutoModelForSequenceClassification.from_pretrained("clapAI/roberta-large-multilingual-sentiment")
    return tokenizer, model

@st.cache_resource
def cargar_modelo_embeddings():
    """Carga el modelo de embeddings de SentenceTransformers."""
    model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-mpnet-base-v2")
    return model

def analizar_tono_local(texto: str, tokenizer, model) -> str:
    """Analiza el tono de un texto usando un modelo local."""
    if not texto or not isinstance(texto, str):
        return "Neutro"
    
    try:
        inputs = tokenizer(texto, return_tensors="pt", truncation=True, max_length=512)
        with torch.no_grad():
            logits = model(**inputs).logits
        
        scores = logits.softmax(dim=1)[0].tolist()
        labels = ["Negativo", "Neutro", "Positivo"]
        
        # Mapeo de la salida del modelo a nuestras etiquetas
        # El modelo clapAI tiene el orden: negative, neutral, positive
        label_map = {
            "negative": "Negativo",
            "neutral": "Neutro",
            "positive": "Positivo"
        }
        
        predicted_class_id = torch.argmax(logits, dim=-1).item()
        predicted_label = model.config.id2label[predicted_class_id]
        
        return label_map.get(predicted_label, "Neutro")
        
    except Exception:
        return "Neutro" # Fallback

# ======================================
# ### MODIFICADO: Análisis de Tono y Tema ###
# ======================================

async def _etiquetar_cluster_con_ia(texto_representante: str) -> str:
    """Función auxiliar que llama a OpenAI SÓLO para generar la etiqueta de un cluster."""
    prompt = f"""
    Eres un analista de medios experto en sintetizar información.
    Basado en la siguiente noticia, que es la más representativa de un grupo de noticias similares, crea un tema corto y descriptivo de 3 a 5 palabras.
    El tema debe ser claro, conciso y capturar la esencia del evento principal.

    Ejemplo:
    - Noticia: "El rector de la UNAL, Ismael Peña, se posesionó en una notaría. Hubo protestas de estudiantes en el campus."
    - Tema generado: "Posesión del rector y protestas"

    Noticia a analizar:
    ---
    {texto_representante[:MAX_TOKENS_PROMPT_TXT]}
    ---

    Genera únicamente el tema, sin explicaciones adicionales.
    """
    try:
        resp = await openai.ChatCompletion.acreate(
            model=OPENAI_MODEL_ETIQUETADO,
            messages=[
                {"role": "system", "content": "Generas temas cortos y descriptivos para grupos de noticias."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=20,
            temperature=0.1,
        )
        tema = resp.choices[0].message.content.strip().strip('"')
        return limpiar_tema(tema)
    except Exception:
        return "Tema no disponible"

async def generar_y_etiquetar_temas_local(noticias: List[Dict], key_map: Dict[str, str], p_bar) -> Dict[int, str]:
    """
    Genera embeddings, agrupa en clusters y luego etiqueta cada cluster para definir los temas.
    Devuelve un mapeo de original_index -> tema_asignado.
    """
    p_bar.progress(0.1, text="🧠 Generando embeddings con modelo local...")
    modelo_emb = cargar_modelo_embeddings()
    
    # Prepara los textos para los embeddings
    textos_para_embed = [
        corregir_texto(n.get(key_map["titulo"], "")) + ". " + corregir_texto(n.get(key_map["resumen"], ""))
        for n in noticias
    ]
    
    # Genera embeddings en lotes para manejar la memoria
    embeddings = modelo_emb.encode(textos_para_embed, show_progress_bar=True, batch_size=32)
    
    p_bar.progress(0.5, f"🔄 Agrupando noticias en {NUM_TEMAS_CLUSTERING} temas...")
    
    # Clustering con K-Means
    kmeans = KMeans(n_clusters=NUM_TEMAS_CLUSTERING, random_state=42, n_init='auto')
    kmeans.fit(embeddings)
    
    # Asigna cada noticia a un cluster
    for i, noticia in enumerate(noticias):
        noticia['cluster_id'] = kmeans.labels_[i]

    p_bar.progress(0.7, f"✍️ Etiquetando los {NUM_TEMAS_CLUSTERING} temas con IA...")
    
    # Etiquetar cada cluster usando la noticia más cercana al centroide
    mapa_cluster_a_tema = {}
    tasks = []
    
    for cluster_id in range(NUM_TEMAS_CLUSTERING):
        indices_cluster = [i for i, n in enumerate(noticias) if n['cluster_id'] == cluster_id]
        if not indices_cluster:
            continue
            
        # Encontrar la noticia más representativa (cercana al centroide)
        embeddings_cluster = embeddings[indices_cluster]
        centroide = kmeans.cluster_centers_[cluster_id]
        distancias = np.linalg.norm(embeddings_cluster - centroide, axis=1)
        indice_representante_local = np.argmin(distancias)
        indice_representante_global = indices_cluster[indice_representante_local]
        
        texto_rep = textos_para_embed[indice_representante_global]
        
        # Crear tarea para etiquetar con OpenAI
        tasks.append(_etiquetar_cluster_con_ia(texto_rep))

    # Ejecutar todas las llamadas a la API de etiquetado en paralelo
    etiquetas_temas = await asyncio.gather(*tasks)
    
    for i, tema in enumerate(etiquetas_temas):
        mapa_cluster_a_tema[i] = tema
        
    p_bar.progress(0.9, "✅ Etiquetado completado. Asignando temas...")

    # Mapeo final: original_index -> tema
    mapa_final_temas = {}
    for noticia in noticias:
        idx = noticia['original_index']
        cluster = noticia['cluster_id']
        mapa_final_temas[idx] = mapa_cluster_a_tema.get(cluster, "Tema no asignado")
        
    return mapa_final_temas


# ======================================
# ... (Aquí van tus funciones existentes sin cambios)
# detectar_duplicados_avanzado, run_base_logic, process_mappings_and_links,
# process_sov_mapping_final, _append_rows_to_sheet, generate_two_sheet_excel
# ======================================

# ... (Asegúrate de copiar esas funciones aquí) ...

# ======================================
# ### MODIFICADO: Proceso Principal y UI ###
# ======================================
async def run_full_process_async(dossier_file, region_file, internet_file, sov_file, brand_aliases, emb_cos_thr):
    try:
        openai.api_key = st.secrets["OPENAI_API_KEY"]
    except Exception:
        st.error("❌ Error: OPENAI_API_KEY no encontrado. Es necesario para el etiquetado de temas.")
        st.stop()

    with st.status("📋 **Paso 1/3:** Limpieza, duplicados y mapeos...", expanded=True) as s:
        # Esta parte no cambia
        all_processed_rows, key_map = run_base_logic(load_workbook(dossier_file, data_only=True).active)
        all_processed_rows = process_mappings_and_links(all_processed_rows, key_map, region_file, internet_file)
        s.update(label="✅ **Paso 1/3:** Base de datos preparada", state="complete")
    
    for row in all_processed_rows:
        row["__is_target_brand"] = (row.get(key_map.get("menciones")) in TARGET_BRANDS)

    index_to_row_map = {row['original_index']: row for row in all_processed_rows}

    target_rows_all = [row for row in all_processed_rows if row.get("__is_target_brand") and not row.get("is_duplicate")]

    if not target_rows_all:
        st.warning("No se encontraron noticias únicas para las marcas objetivo para analizar.")
    else:
        with st.status("🧠 **Paso 2/3:** Analizando Tono y Tema con modelos locales...", expanded=True) as s:
            p_bar = st.progress(0, text="🚀 Preparando modelos locales...")
            
            # Cargar modelos de sentimiento
            tokenizer_sent, model_sent = cargar_modelo_sentimiento()
            
            # 1. Analizar Tono para todas las noticias objetivo
            p_bar.progress(0.05, text=f"📊 Analizando tono para {len(target_rows_all)} noticias...")
            for i, row in enumerate(target_rows_all):
                texto = corregir_texto(row.get(key_map["titulo"], "")) + ". " + corregir_texto(row.get(key_map["resumen"], ""))
                tono = analizar_tono_local(texto, tokenizer_sent, model_sent)
                index_to_row_map[row['original_index']][key_map["tonoai"]] = tono
                if (i + 1) % 50 == 0:
                    p_bar.progress(0.05 + (i / len(target_rows_all)) * 0.15, text=f"📊 Analizando tono... {i+1}/{len(target_rows_all)}")

            # 2. Generar, agrupar y etiquetar temas
            mapa_idx_a_tema = await generar_y_etiquetar_temas_local(target_rows_all, key_map, p_bar)
            
            # Asignar los temas finales
            for idx, tema in mapa_idx_a_tema.items():
                index_to_row_map[idx][key_map["tema"]] = tema

            s.update(label="✅ **Paso 2/3:** Análisis local completado", state="complete")

    with st.status("📊 **Paso 3/3:** Aplicando SOV y generando informe final...", expanded=True) as s:
        final_processed_rows = list(index_to_row_map.values())
        final_processed_rows = process_sov_mapping_final(final_processed_rows, key_map, sov_file)
        st.session_state["output_data"] = generate_two_sheet_excel(final_processed_rows, key_map)
        st.session_state["output_filename"] = f"Informe_Analisis_UNAL_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        st.session_state["processing_complete"] = True
        s.update(label="✅ **Paso 3/3:** Informe generado exitosamente", state="complete")

# ### MODIFICADO: La función main tiene cambios menores en los textos de ayuda ###
def main():
    load_custom_css()
    if not check_password():
        return

    st.markdown('<div class="main-header">🎓 Sistema de Análisis de Noticias para la Universidad Nacional</div>', unsafe_allow_html=True)
    st.markdown(
        "Esta herramienta utiliza **modelos de IA locales** para analizar Tono y Tema en las noticias de 'U. Nacional de Colombia' y 'Universidad Nacional de Colombia'. "
        "Los temas se generan agrupando noticias similares en 20 categorías principales."
    )

    if not st.session_state.get("processing_complete", False):
        with st.form("input_form"):
            st.markdown("### 📂 Archivos de Entrada")
            col1, col2, col3, col4 = st.columns(4)
            dossier_file = col1.file_uploader("**1. Dossier Principal** (.xlsx)", type=["xlsx"])
            region_file = col2.file_uploader("**2. Mapeo de Región** (.xlsx)", type=["xlsx"])
            internet_file = col3.file_uploader("**3. Mapeo Internet** (.xlsx)", type=["xlsx"])
            sov_file = col4.file_uploader("**4. Mapeo SOV** (.xlsx)", type=["xlsx"])

            st.info("El análisis de IA se ejecutará automáticamente solo para 'U. Nacional de Colombia' y 'Universidad Nacional de Colombia'.")
            
            # Los alias ya no son tan relevantes para el prompt de IA, pero se pueden mantener para otros usos
            brand_aliases_text = st.text_area(
                "**Alias y voceros (opcional)**",
                value="UNAL;UN;U. Nacional;Universidad Nacional;Ismael Peña",
                height=80,
                help="Estos valores actualmente no se usan en el análisis con modelos locales, pero se conservan para futuras funcionalidades."
            )

            # El slider de consolidación ya no es necesario con el método de clustering
            st.markdown("### ⚙️ Parámetros de Análisis")
            st.write(f"El sistema agrupará las noticias en **{NUM_TEMAS_CLUSTERING} temas principales** de forma automática.")
            
            # Placeholder para que el layout no se rompa si se usaba emb_cos_thr en otro lado
            emb_cos_thr_placeholder = 0.88 

            if st.form_submit_button("🚀 **INICIAR ANÁLISIS COMPLETO**", use_container_width=True, type="primary"):
                if not all([dossier_file, region_file, internet_file, sov_file]):
                    st.error("❌ Faltan archivos obligatorios (incluya el Mapeo SOV).")
                else:
                    aliases = [a.strip() for a in brand_aliases_text.split(";") if a.strip()]
                    # El slider ya no se pasa como parámetro
                    asyncio.run(run_full_process_async(dossier_file, region_file, internet_file, sov_file, aliases, emb_cos_thr_placeholder))
                    st.rerun()
    else:
        st.success("## 🎉 Análisis Completado Exitosamente")
        st.markdown("El informe en Excel ha sido generado con dos pestañas: **'UNAL con IA'** (solo marcas objetivo) y **'Todas las Marcas'**.")
        st.download_button(
            label="📥 **DESCARGAR INFORME**",
            data=st.session_state.output_data,
            file_name=st.session_state.output_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            type="primary"
        )
        if st.button("🔄 **Realizar un Nuevo Análisis**", use_container_width=True):
            pwd = st.session_state.get("password_correct")
            st.session_state.clear()
            st.session_state.password_correct = pwd
            st.rerun()

    st.markdown("<hr><div style='text-align:center;color:#666;font-size:0.9rem;'><p>Sistema de Análisis de Noticias v8.0.0 (Local Models Edition) | Adaptado para la Universidad Nacional</p></div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()

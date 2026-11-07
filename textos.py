"""Utilidades de texto sin dependencias de Streamlit (se pueden probar de forma aislada)."""
import re


def normalize_title_for_comparison(title) -> str:
    """
    Normaliza un título para detectar duplicados.

    Solo descarta el sufijo del medio cuando es corto ("Título | El Tiempo", "Título - Semana"),
    pero conserva el título completo. La versión anterior se quedaba con lo que había ANTES del
    primer ":" o "-" (incluso en "Covid-19"), de modo que "Universidad Nacional: abre becas" y
    "Universidad Nacional: estudiantes bloquean la entrada" quedaban iguales y se trataban como
    la misma noticia (falsos duplicados y tono/tema propagado a noticias distintas).
    """
    if not isinstance(title, str):
        return ""
    partes = re.split(r"\s+[|–—-]\s+", title.strip())
    if len(partes) > 1 and len(partes[-1].split()) <= 4:
        partes = partes[:-1]
    return re.sub(r"\W+", " ", " ".join(partes)).lower().strip()

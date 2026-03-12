import os
import re

BASE = os.path.dirname(os.path.abspath(__file__))
PDF_ROOT = os.path.join(BASE, "pdfs")


def _safe_segment(value):
    text = re.sub(r"[^0-9A-Za-z._-]+", "_", str(value or ""))
    return text.strip("._-") or "sin_valor"


def pdf_tipo_dir(tipo):
    tipo_n = (tipo or "").strip().lower()
    if tipo_n in {"circular", "circulares", "circular_historica", "circulares_historicas"}:
        return "circular"
    if tipo_n in {"oficio", "oficios", "oficio_iva", "oficio_lir", "oficio_otras"}:
        return "oficio"
    if tipo_n in {"resolucion", "resoluciones"}:
        return "resolucion"
    if tipo_n in {"judicial", "jurisprudencia_judicial"}:
        return "judicial"
    return _safe_segment(tipo_n or "otros")


def ensure_pdf_year_dir(tipo, anio):
    year_dir = os.path.join(PDF_ROOT, pdf_tipo_dir(tipo), str(anio))
    os.makedirs(year_dir, exist_ok=True)
    return year_dir


def build_pdf_path(tipo, anio, filename):
    return os.path.join(ensure_pdf_year_dir(tipo, anio), filename)

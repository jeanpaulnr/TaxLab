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


def pdf_categoria_dir(tipo, categoria=None):
    tipo_n = (tipo or "").strip().lower()
    categoria_n = (categoria or "").strip().lower()

    if pdf_tipo_dir(tipo_n) != "oficio":
        return ""

    if categoria_n in {"oficio_lir", "lir", "renta"} or tipo_n == "oficio_lir":
        return "lir"
    if categoria_n in {"oficio_iva", "iva"} or tipo_n == "oficio_iva":
        return "iva"
    if categoria_n in {"oficio_otras", "otras", "otras_normas", "otras normas"} or tipo_n == "oficio_otras":
        return "otras_normas"
    return ""


def ensure_pdf_year_dir(tipo, anio, categoria=None):
    parts = [PDF_ROOT, pdf_tipo_dir(tipo)]
    categoria_dir = pdf_categoria_dir(tipo, categoria)
    if categoria_dir:
        parts.append(categoria_dir)
    parts.append(str(anio))
    year_dir = os.path.join(*parts)
    os.makedirs(year_dir, exist_ok=True)
    return year_dir


def build_pdf_path(tipo, anio, filename, categoria=None):
    return os.path.join(ensure_pdf_year_dir(tipo, anio, categoria=categoria), filename)

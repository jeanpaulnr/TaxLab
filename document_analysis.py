import json
import os
import re
import sqlite3
from datetime import datetime

import requests

from normativa_refs import exact_article_refs, normalize_body_code, parse_article_ref_list

MODEL = os.getenv('DOCUMENT_ANALYSIS_MODEL', 'claude-sonnet-4-20250514')
PROMPT_VERSION = 'document-analysis-v1'
ANTHROPIC_URL = 'https://api.anthropic.com/v1/messages'

SYSTEM_PROMPT = """Eres un analista tributario chileno.
Debes analizar UN SOLO documento del SII o judicial y producir una salida
estructurada, fundada y util para una ficha documental profesional.

REGLAS:
1. Basa toda afirmacion en el documento entregado.
2. No inventes normas, articulos ni conclusiones.
3. Si la evidencia es insuficiente, dilo expresamente.
4. No confundas identidad del documento con tema tratado.
5. Devuelve JSON estricto.

JSON:
{
  "summary_short": "resumen ejecutivo breve",
  "summary_technical": "sintesis tecnica mas desarrollada",
  "question_resolved": "pregunta o controversia que aborda",
  "holding_principal": "criterio o conclusion principal",
  "implicancia_practica": "impacto practico para trabajo tributario",
  "normas_citadas": ["LIR", "LIVS"],
  "articulos_citados": [{"cuerpo":"LIR","articulo":"31","clave":"LIR:31","label":"Art. 31 LIR","slug":"lir-art-31"}],
  "evidence": [{"label":"Cita 1","excerpt":"..."}],
  "confidence": "alta|media|baja",
  "notes": "advertencias o limites"
}
"""


def get_connection(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _load_document(conn, doc_id):
    row = conn.execute(
        """
        SELECT id, hash_md5, tipo, numero, anio, fecha, titulo, materia, subtema,
               contenido, resumen, referencia, leyes_citadas, articulos_clave,
               chars_texto, pdf_local, url_sii
        FROM documentos
        WHERE id = ?
        """,
        (doc_id,),
    ).fetchone()
    return dict(row) if row else None


def _load_existing(conn, doc_id):
    row = conn.execute(
        "SELECT * FROM document_analysis WHERE doc_id = ?",
        (doc_id,),
    ).fetchone()
    return dict(row) if row else None


def _clean_text(text):
    text = re.sub(r'\s+\n', '\n', (text or '').strip())
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _extract_evidence(text, max_items=3):
    text = _clean_text(text)
    blocks = []
    seen = set()
    for chunk in re.split(r'\n{2,}', text):
        chunk = re.sub(r'\s+', ' ', chunk).strip()
        if len(chunk) < 120:
            continue
        key = chunk[:160]
        if key in seen:
            continue
        seen.add(key)
        blocks.append(chunk[:500])
        if len(blocks) >= max_items:
            break
    return [{'label': f'Cita {idx + 1}', 'excerpt': value} for idx, value in enumerate(blocks)]


def _normalize_laws(values):
    seen = set()
    result = []
    for value in values or []:
        code = normalize_body_code(value)
        if not code or code in seen:
            continue
        seen.add(code)
        result.append(code)
    return result


def _safe_json_loads(value, default=None):
    if default is None:
        default = []
    if not value:
        return list(default) if isinstance(default, list) else default
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return list(default) if isinstance(default, list) else default


def _build_fallback(doc):
    content = _clean_text(doc.get('contenido'))
    summary = _clean_text(doc.get('resumen') or '')
    refs = exact_article_refs(doc.get('articulos_clave'))
    laws = _normalize_laws(_safe_json_loads(doc.get('leyes_citadas')))
    theme = (doc.get('titulo') or doc.get('referencia') or '').strip()
    summary_short = summary or content[:700]
    summary_technical = summary or content[:1800]
    return {
        'status': 'fallback',
        'model': 'local-fallback',
        'prompt_version': PROMPT_VERSION,
        'summary_short': summary_short.strip(),
        'summary_technical': summary_technical.strip(),
        'question_resolved': theme,
        'holding_principal': summary_short.strip()[:1200],
        'implicancia_practica': 'Analisis automatico no disponible; revisar el PDF y el texto canonico del documento.',
        'normas_citadas': laws,
        'articulos_citados': refs,
        'evidence': _extract_evidence(content),
        'confidence': 'baja',
        'notes': 'Analisis generado sin modelo externo; sirve como base editable y trazable.',
    }


def _extract_json(text):
    text = (text or '').strip()
    if not text:
        raise ValueError('Respuesta vacia del modelo')
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if not match:
        raise ValueError('No se encontro JSON en la respuesta del modelo')
    return json.loads(match.group(0))


def _call_model(api_key, doc):
    content = _clean_text(doc.get('contenido'))
    payload = {
        'model': MODEL,
        'max_tokens': 1600,
        'system': SYSTEM_PROMPT,
        'messages': [
            {
                'role': 'user',
                'content': (
                    f"Documento: {doc.get('referencia') or doc.get('titulo')}\n"
                    f"Tipo: {doc.get('tipo')} | Fecha: {doc.get('fecha') or doc.get('anio')}\n"
                    f"Leyes citadas detectadas: {doc.get('leyes_citadas') or '[]'}\n"
                    f"Articulos detectados: {doc.get('articulos_clave') or '[]'}\n\n"
                    "Texto canonico completo del documento:\n"
                    f"{content}\n\n"
                    "Devuelve solo JSON estricto."
                ),
            }
        ],
    }
    response = requests.post(
        ANTHROPIC_URL,
        headers={
            'x-api-key': api_key,
            'anthropic-version': '2023-06-01',
            'content-type': 'application/json',
        },
        json=payload,
        timeout=120,
    )
    response.raise_for_status()
    data = response.json()
    raw = '\n'.join(
        item.get('text', '')
        for item in data.get('content', [])
        if item.get('type') == 'text'
    )
    parsed = _extract_json(raw)
    return {
        'status': 'ready',
        'model': MODEL,
        'prompt_version': PROMPT_VERSION,
        'summary_short': (parsed.get('summary_short') or '').strip(),
        'summary_technical': (parsed.get('summary_technical') or '').strip(),
        'question_resolved': (parsed.get('question_resolved') or '').strip(),
        'holding_principal': (parsed.get('holding_principal') or '').strip(),
        'implicancia_practica': (parsed.get('implicancia_practica') or '').strip(),
        'normas_citadas': _normalize_laws(parsed.get('normas_citadas') or []),
        'articulos_citados': parse_article_ref_list(parsed.get('articulos_citados') or [], include_ambiguous=False),
        'evidence': parsed.get('evidence') or _extract_evidence(doc.get('contenido')),
        'confidence': str(parsed.get('confidence') or 'media').lower(),
        'notes': (parsed.get('notes') or '').strip(),
    }


def _persist_analysis(conn, doc, payload):
    source_hash = f"{doc.get('hash_md5') or ''}:{doc.get('chars_texto') or 0}"
    now = datetime.now().isoformat()
    conn.execute(
        """
        INSERT INTO document_analysis(
            doc_id, status, source_hash, model, prompt_version,
            summary_short, summary_technical, question_resolved,
            holding_principal, implicancia_practica,
            normas_citadas_json, articulos_citados_json, evidence_json,
            confidence, notes, generated_at, updated_at
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(doc_id) DO UPDATE SET
            status=excluded.status,
            source_hash=excluded.source_hash,
            model=excluded.model,
            prompt_version=excluded.prompt_version,
            summary_short=excluded.summary_short,
            summary_technical=excluded.summary_technical,
            question_resolved=excluded.question_resolved,
            holding_principal=excluded.holding_principal,
            implicancia_practica=excluded.implicancia_practica,
            normas_citadas_json=excluded.normas_citadas_json,
            articulos_citados_json=excluded.articulos_citados_json,
            evidence_json=excluded.evidence_json,
            confidence=excluded.confidence,
            notes=excluded.notes,
            generated_at=excluded.generated_at,
            updated_at=excluded.updated_at
        """,
        (
            doc['id'],
            payload['status'],
            source_hash,
            payload.get('model'),
            payload.get('prompt_version'),
            payload.get('summary_short'),
            payload.get('summary_technical'),
            payload.get('question_resolved'),
            payload.get('holding_principal'),
            payload.get('implicancia_practica'),
            json.dumps(payload.get('normas_citadas') or [], ensure_ascii=False),
            json.dumps(payload.get('articulos_citados') or [], ensure_ascii=False),
            json.dumps(payload.get('evidence') or [], ensure_ascii=False),
            payload.get('confidence'),
            payload.get('notes'),
            now,
            now,
        ),
    )
    conn.commit()


def generate_document_analysis(db_path, doc_id, force=False):
    conn = get_connection(db_path)
    try:
        doc = _load_document(conn, doc_id)
        if not doc:
            raise ValueError('Documento no encontrado')

        existing = _load_existing(conn, doc_id)
        source_hash = f"{doc.get('hash_md5') or ''}:{doc.get('chars_texto') or 0}"
        if existing and not force and existing.get('source_hash') == source_hash and existing.get('status') in {'ready', 'fallback'}:
            existing['normas_citadas'] = _normalize_laws(json.loads(existing.get('normas_citadas_json') or '[]'))
            existing['articulos_citados'] = parse_article_ref_list(existing.get('articulos_citados_json') or '[]', include_ambiguous=False)
            existing['evidence'] = json.loads(existing.get('evidence_json') or '[]')
            return existing

        if not (doc.get('contenido') or '').strip() or int(doc.get('chars_texto') or 0) < 400:
            payload = {
                'status': 'insufficient',
                'model': 'local-fallback',
                'prompt_version': PROMPT_VERSION,
                'summary_short': '',
                'summary_technical': '',
                'question_resolved': doc.get('referencia') or doc.get('titulo') or '',
                'holding_principal': '',
                'implicancia_practica': '',
            'normas_citadas': _normalize_laws(_safe_json_loads(doc.get('leyes_citadas'))),
                'articulos_citados': exact_article_refs(doc.get('articulos_clave')),
                'evidence': [],
                'confidence': 'baja',
                'notes': 'Documento con texto insuficiente para analisis confiable.',
            }
        else:
            api_key = os.getenv('ANTHROPIC_API_KEY', '').strip()
            if api_key:
                try:
                    payload = _call_model(api_key, doc)
                except Exception as exc:
                    payload = _build_fallback(doc)
                    payload['notes'] = f"{payload.get('notes', '')} Fallback aplicado por error del modelo: {exc}".strip()
            else:
                payload = _build_fallback(doc)

        _persist_analysis(conn, doc, payload)
        saved = _load_existing(conn, doc_id)
        saved = dict(saved) if saved else {}
        saved['normas_citadas'] = _normalize_laws(_safe_json_loads(saved.get('normas_citadas_json')))
        saved['articulos_citados'] = parse_article_ref_list(saved.get('articulos_citados_json') or '[]', include_ambiguous=False)
        saved['evidence'] = _safe_json_loads(saved.get('evidence_json'))
        return saved
    finally:
        conn.close()

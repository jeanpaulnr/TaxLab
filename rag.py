import json
import os
import re
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import requests

SYSTEM_PROMPT = """Eres un asistente experto en derecho tributario chileno.
Tu rol es analizar consultas tributarias y responder EXCLUSIVAMENTE con base
en la normativa y jurisprudencia que se te proporciona como contexto.

REGLAS OBLIGATORIAS:
1. NUNCA inventes información. Si no hay evidencia suficiente en el contexto,
   di explícitamente: \"No encontré base suficiente en el corpus para afirmar esto.\"
2. SIEMPRE cita la fuente exacta: tipo de documento, número, año, y artículos relevantes.
3. SEPARA claramente:
   - Lo que dice la norma
   - Lo que dice el criterio administrativo del SII
   - Lo que dice la jurisprudencia judicial
   - Tu interpretación o sugerencia
4. Si hay criterios contradictorios entre documentos, señálalos explícitamente.
5. Si un criterio fue modificado o reemplazado en el tiempo, indica la evolución.
6. Responde en español formal pero accesible para un profesional tributario chileno.

Devuelve JSON estricto con estas claves:
respuesta, fundamento, riesgos, confianza, nota.
"""

MODEL = 'claude-sonnet-4-20250514'
ANTHROPIC_URL = 'https://api.anthropic.com/v1/messages'


def get_connection(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _build_fts_query(pregunta: str) -> str:
    palabras = [p.strip() for p in re.split(r'\s+', pregunta) if p.strip()]
    piezas = []
    for palabra in palabras:
        if len(palabra) > 3:
            piezas.append(f'"{palabra}"')
        else:
            piezas.append(palabra)
    return ' OR '.join(piezas) or pregunta


def _extract_fragment(texto: str, pregunta: str, max_chars: int = 900) -> str:
    texto = (texto or '').strip()
    if not texto:
        return ''
    palabras = [p for p in re.split(r'\W+', pregunta.lower()) if len(p) > 3]
    lower = texto.lower()
    idx = -1
    for palabra in palabras:
        idx = lower.find(palabra)
        if idx >= 0:
            break
    if idx < 0:
        return texto[:max_chars]
    start = max(0, idx - max_chars // 3)
    end = min(len(texto), start + max_chars)
    return texto[start:end].strip()


def buscar_contexto_rag(db_path: str, pregunta: str, filtros: Optional[Dict[str, Any]] = None,
                       max_docs: int = 8, max_chars_total: int = 9000) -> Tuple[str, List[Dict[str, Any]]]:
    filtros = filtros or {}
    conn = get_connection(db_path)
    try:
        where = ["docs_fts MATCH ?"]
        params: List[Any] = [_build_fts_query(pregunta)]

        if filtros.get('tipo'):
            where.append('d.tipo = ?')
            params.append(filtros['tipo'])
        if filtros.get('anio'):
            where.append('d.anio = ?')
            params.append(int(filtros['anio']))
        if filtros.get('ley'):
            where.append('d.leyes_citadas LIKE ?')
            params.append(f'%"{filtros["ley"]}"%')

        sql = f"""
        SELECT d.id, d.tipo, d.numero, d.anio, d.fecha, d.referencia, d.titulo,
               d.leyes_citadas, d.articulos_clave, d.resumen, d.contenido,
               bm25(docs_fts) AS rank
        FROM docs_fts
        JOIN documentos d ON d.id = docs_fts.rowid
        WHERE {' AND '.join(where)}
        ORDER BY rank
        LIMIT ?
        """
        rows = conn.execute(sql, params + [max_docs]).fetchall()
    except Exception:
        like = f'%{pregunta.strip()}%'
        sql = """
        SELECT id, tipo, numero, anio, fecha, referencia, titulo,
               leyes_citadas, articulos_clave, resumen, contenido,
               1000.0 AS rank
        FROM documentos
        WHERE titulo LIKE ? OR resumen LIKE ? OR contenido LIKE ?
        ORDER BY fecha DESC, id DESC
        LIMIT ?
        """
        rows = conn.execute(sql, [like, like, like, max_docs]).fetchall()
    finally:
        conn.close()

    fuentes: List[Dict[str, Any]] = []
    bloques: List[str] = []
    chars_usados = 0

    for row in rows:
        doc = dict(row)
        extracto = _extract_fragment(doc.get('resumen') or doc.get('contenido') or '', pregunta)
        leyes = doc.get('leyes_citadas') or '[]'
        articulos = doc.get('articulos_clave') or '[]'
        bloque = (
            f"--- DOCUMENTO {len(fuentes) + 1} ---\n"
            f"Tipo: {doc.get('tipo')} | Numero: {doc.get('numero') or '-'} | "
            f"Anio: {doc.get('anio') or '-'} | Fecha: {doc.get('fecha') or '-'}\n"
            f"Referencia: {doc.get('referencia') or doc.get('titulo')}\n"
            f"Leyes citadas: {leyes}\n"
            f"Articulos: {articulos}\n"
            f"Extracto relevante:\n{extracto}\n"
        )
        if chars_usados + len(bloque) > max_chars_total:
            break
        chars_usados += len(bloque)
        bloques.append(bloque)
        relevancia = 1.0 / (1.0 + abs(float(doc.get('rank') or 0)))
        fuentes.append({
            'id': doc['id'],
            'tipo': doc['tipo'],
            'referencia': doc.get('referencia') or doc.get('titulo'),
            'extracto': extracto,
            'relevancia': round(relevancia, 4),
        })

    return '\n'.join(bloques), fuentes


def _fallback_response(fuentes: List[Dict[str, Any]], nota: str) -> Dict[str, Any]:
    return {
        'respuesta': 'No encontré base suficiente en el corpus para afirmar esto.' if not fuentes else 'Encontré documentos relevantes, pero no tengo salida del modelo disponible en este entorno.',
        'fundamento': '',
        'fuentes': fuentes,
        'riesgos': 'La respuesta automática requiere modelo configurado y evidencia suficiente.',
        'confianza': 'baja' if not fuentes else 'media',
        'nota': nota,
    }


def _call_claude(api_key: str, pregunta: str, contexto: str) -> Dict[str, Any]:
    payload = {
        'model': MODEL,
        'max_tokens': 1800,
        'system': SYSTEM_PROMPT,
        'messages': [
            {
                'role': 'user',
                'content': (
                    'Consulta del usuario:\n'
                    f'{pregunta}\n\n'
                    'Contexto documental:\n'
                    f'{contexto}\n\n'
                    'Responde en JSON estricto con las claves respuesta, fundamento, riesgos, confianza y nota.'
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
        timeout=90,
    )
    response.raise_for_status()
    data = response.json()
    text_parts = []
    for item in data.get('content', []):
        if item.get('type') == 'text':
            text_parts.append(item.get('text', ''))
    raw = '\n'.join(text_parts).strip()
    return json.loads(raw)


def responder_consulta_tributaria(db_path: str, pregunta: str, filtros: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    pregunta = (pregunta or '').strip()
    if not pregunta:
        return _fallback_response([], 'Debes ingresar una pregunta.')

    contexto, fuentes = buscar_contexto_rag(db_path, pregunta, filtros=filtros)
    if not contexto:
        return _fallback_response([], 'No encontré documentos relevantes para construir evidencia.')

    api_key = os.getenv('ANTHROPIC_API_KEY', '').strip()
    if not api_key:
        respuesta = _fallback_response(fuentes, 'ANTHROPIC_API_KEY no configurada. Se devolvieron solo las fuentes encontradas.')
        return respuesta

    try:
        model_output = _call_claude(api_key, pregunta, contexto)
    except Exception as exc:
        return _fallback_response(fuentes, f'No pude consultar el modelo: {exc}')

    return {
        'respuesta': model_output.get('respuesta', ''),
        'fundamento': model_output.get('fundamento', ''),
        'fuentes': fuentes,
        'riesgos': model_output.get('riesgos', ''),
        'confianza': str(model_output.get('confianza', 'media')).lower(),
        'nota': model_output.get('nota', ''),
    }

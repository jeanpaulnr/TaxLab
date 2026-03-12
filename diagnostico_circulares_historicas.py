# -*- coding: utf-8 -*-
"""
Diagnostico de paridad para circulares historicas del SII.

Compara, por anio, lo publicado en el indice oficial del SII versus lo que
ya existe en la tabla `documentos` para `tipo='circular'`.

Uso:
  python diagnostico_circulares_historicas.py --anio 2012
  python diagnostico_circulares_historicas.py --desde 1995 --hasta 2012
"""

import argparse
import json
import os
import sqlite3
import time
from collections import Counter
from datetime import datetime

from descargar_circulares_historicas import BASE, descargar_indice, descubrir_circulares


DB = os.path.join(BASE, 'data', 'sii_normativa.db')
LOGS_DIR = os.path.join(BASE, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)


def normalizar_numero(numero):
    valor = str(numero or '').strip().lower()
    if not valor:
        return ''

    valor = (
        valor.replace('n.', '')
        .replace('n ', '')
        .replace('n°', '')
        .replace('nº', '')
        .replace('nro.', '')
        .replace('numero', '')
        .strip()
    )

    prefijo = ''
    sufijo = ''
    for ch in valor:
        if ch.isdigit() and not sufijo:
            prefijo += ch
        else:
            sufijo += ch

    if prefijo:
        try:
            prefijo = str(int(prefijo))
        except ValueError:
            pass

    limpio = f'{prefijo}{sufijo}'.strip()
    return limpio or valor


def obtener_db_por_anio(conn, anio):
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        '''
        SELECT id, numero, fuente, titulo
        FROM documentos
        WHERE tipo='circular' AND anio=?
        ORDER BY id
        ''',
        (anio,),
    ).fetchall()

    normalizados = []
    detalle = []
    for row in rows:
        numero_norm = normalizar_numero(row['numero'])
        normalizados.append(numero_norm)
        detalle.append(
            {
                'id': row['id'],
                'numero': row['numero'],
                'numero_norm': numero_norm,
                'fuente': row['fuente'],
                'titulo': row['titulo'],
            }
        )

    conteo = Counter(normalizados)
    duplicados = sorted([n for n, c in conteo.items() if n and c > 1], key=str)
    unicos = sorted({n for n in normalizados if n}, key=str)
    return {
        'rows': detalle,
        'conteo_total': len(rows),
        'numeros_unicos': unicos,
        'duplicados': duplicados,
    }


def obtener_indice_por_anio(anio, reintentos=3, pausa=1.0):
    ultimo_error = None
    for intento in range(1, reintentos + 1):
        try:
            index_url, html, encoding = descargar_indice(anio)
            if not index_url:
                ultimo_error = 'indice_no_encontrado'
            else:
                docs = descubrir_circulares(anio, index_url, html)
                numeros = sorted(
                    {normalizar_numero(doc.get('numero')) for doc in docs if normalizar_numero(doc.get('numero'))},
                    key=str,
                )
                return {
                    'ok': True,
                    'index_url': index_url,
                    'encoding': encoding,
                    'docs': docs,
                    'numeros_unicos': numeros,
                }
        except Exception as exc:
            ultimo_error = str(exc)

        if intento < reintentos:
            time.sleep(pausa)

    return {
        'ok': False,
        'error': ultimo_error or 'desconocido',
        'index_url': None,
        'encoding': None,
        'docs': [],
        'numeros_unicos': [],
    }


def diagnosticar_anio(conn, anio, reintentos=3, pausa=1.0):
    indice = obtener_indice_por_anio(anio, reintentos=reintentos, pausa=pausa)
    db = obtener_db_por_anio(conn, anio)

    sii_set = set(indice['numeros_unicos'])
    db_set = set(db['numeros_unicos'])

    faltantes = sorted(sii_set - db_set, key=str)
    extras = sorted(db_set - sii_set, key=str)

    return {
        'anio': anio,
        'index_url': indice['index_url'],
        'encoding': indice['encoding'],
        'sii_ok': indice['ok'],
        'sii_error': indice.get('error'),
        'sii_total': len(indice['docs']),
        'sii_unicos': len(indice['numeros_unicos']),
        'db_total': db['conteo_total'],
        'db_unicos': len(db['numeros_unicos']),
        'faltantes': faltantes,
        'extras': extras,
        'duplicados_db': db['duplicados'],
    }


def imprimir_resumen(resultado):
    anio = resultado['anio']
    if not resultado['sii_ok']:
        print(f'[{anio}] ERROR indice SII -> {resultado["sii_error"]}')
        return

    print(
        f'[{anio}] SII={resultado["sii_unicos"]:>3} | '
        f'BD={resultado["db_unicos"]:>3} | '
        f'faltan={len(resultado["faltantes"]):>3} | '
        f'extras={len(resultado["extras"]):>3} | '
        f'duplicados={len(resultado["duplicados_db"]):>3}'
    )

    if resultado['faltantes']:
        muestra = ', '.join(resultado['faltantes'][:20])
        print(f'   faltantes: {muestra}')
    if resultado['extras']:
        muestra = ', '.join(resultado['extras'][:20])
        print(f'   extras: {muestra}')
    if resultado['duplicados_db']:
        muestra = ', '.join(resultado['duplicados_db'][:20])
        print(f'   duplicados_db: {muestra}')


def guardar_reporte(resultados):
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    path = os.path.join(LOGS_DIR, f'paridad_circulares_historicas_{stamp}.json')
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(resultados, fh, ensure_ascii=False, indent=2)
    return path


def main():
    parser = argparse.ArgumentParser(description='Compara indices historicos SII vs BD local')
    parser.add_argument('--anio', type=int, help='Diagnosticar solo un anio')
    parser.add_argument('--desde', type=int, default=1995, help='Anio desde')
    parser.add_argument('--hasta', type=int, default=2012, help='Anio hasta')
    parser.add_argument('--reintentos', type=int, default=3, help='Reintentos por indice')
    parser.add_argument('--pausa', type=float, default=1.0, help='Pausa entre reintentos')
    args = parser.parse_args()

    anios = [args.anio] if args.anio else list(range(args.hasta, args.desde - 1, -1))

    conn = sqlite3.connect(DB)
    try:
        print('=' * 72)
        print('  PARIDAD DE CIRCULARES HISTORICAS')
        print('=' * 72)

        resultados = []
        for anio in anios:
            resultado = diagnosticar_anio(conn, anio, reintentos=args.reintentos, pausa=args.pausa)
            resultados.append(resultado)
            imprimir_resumen(resultado)

        total_faltantes = sum(len(r['faltantes']) for r in resultados if r['sii_ok'])
        total_extras = sum(len(r['extras']) for r in resultados if r['sii_ok'])
        total_duplicados = sum(len(r['duplicados_db']) for r in resultados if r['sii_ok'])
        anios_incompletos = [r['anio'] for r in resultados if r['sii_ok'] and (r['faltantes'] or r['extras'] or r['duplicados_db'])]
        anios_error = [r['anio'] for r in resultados if not r['sii_ok']]

        reporte = guardar_reporte(resultados)

        print('-' * 72)
        print(f'Anios revisados: {len(resultados)}')
        print(f'Anios con diferencias: {len(anios_incompletos)}')
        print(f'Anios con error de indice: {len(anios_error)}')
        print(f'Total faltantes: {total_faltantes}')
        print(f'Total extras: {total_extras}')
        print(f'Total duplicados en BD: {total_duplicados}')
        print(f'Reporte: {reporte}')
    finally:
        conn.close()


if __name__ == '__main__':
    main()

import argparse
import json
from datetime import datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
REPORTS = ROOT / 'reports'
REPORTS.mkdir(exist_ok=True)
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'scraper'))

from app import init_db
from scraper.engine import scrape_anio
from descargar_resoluciones import descargar_resoluciones
from descargar_circulares_historicas import procesar_anio as procesar_circular_historica


def safe_step(label, fn):
    try:
        result = fn()
        return {'label': label, 'ok': True, 'result': result}
    except Exception as exc:
        return {'label': label, 'ok': False, 'error': str(exc)}


def normalize_counts(result):
    if not isinstance(result, dict):
        return {'total': 0, 'nuevos': 0, 'saltados': 0, 'errores': 1}
    return {
        'total': int(result.get('total', 0) or 0),
        'nuevos': int(result.get('nuevos', 0) or 0),
        'saltados': int(result.get('saltados', 0) or 0),
        'errores': int(result.get('errores', 0) or 0),
    }


def run_year(anio, delay, resolucion_max):
    steps = []

    if anio >= 2013:
        steps.append(safe_step(f'circular/{anio}', lambda: scrape_anio('circular', anio, delay=delay)))
    elif anio >= 1995:
        steps.append(safe_step(f'circular_historica/{anio}', lambda: procesar_circular_historica(anio, solo_indice=False, delay=delay)))

    if anio >= 2013:
        steps.append(safe_step(f'resolucion/{anio}', lambda: descargar_resoluciones(anio, max_num=resolucion_max, delay=max(0.3, delay))))

    if anio >= 2019:
        for tipo in ('oficio_iva', 'oficio_lir', 'oficio_otras'):
            steps.append(safe_step(f'{tipo}/{anio}', lambda tipo=tipo: scrape_anio(tipo, anio, delay=delay)))

    aggregate = {'anio': anio, 'steps': steps, 'total': 0, 'nuevos': 0, 'saltados': 0, 'errores': 0}
    for step in steps:
        if not step['ok']:
            aggregate['errores'] += 1
            continue
        counts = normalize_counts(step['result'])
        aggregate['total'] += counts['total']
        aggregate['nuevos'] += counts['nuevos']
        aggregate['saltados'] += counts['saltados']
        aggregate['errores'] += counts['errores']
    return aggregate


def main():
    parser = argparse.ArgumentParser(description='Reconstruir corpus no judicial 1995-2026')
    parser.add_argument('--desde', type=int, default=1995)
    parser.add_argument('--hasta', type=int, default=2026)
    parser.add_argument('--delay', type=float, default=0.8)
    parser.add_argument('--resolucion-max', type=int, default=300)
    args = parser.parse_args()

    init_db()

    summaries = []
    for anio in range(args.hasta, args.desde - 1, -1):
        print('=' * 72)
        print(f'RECONSTRUYENDO ANIO {anio}')
        print('=' * 72)
        summary = run_year(anio, args.delay, args.resolucion_max)
        summaries.append(summary)
        print(f"Anio {anio}: nuevos={summary['nuevos']} saltados={summary['saltados']} errores={summary['errores']}")

    totals = {
        'anios': len(summaries),
        'nuevos': sum(item['nuevos'] for item in summaries),
        'saltados': sum(item['saltados'] for item in summaries),
        'errores': sum(item['errores'] for item in summaries),
        'total_items': sum(item['total'] for item in summaries),
    }
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = REPORTS / f'reconstruccion_corpus_moderno_{stamp}.json'
    report_path.write_text(json.dumps({'args': vars(args), 'totals': totals, 'years': summaries}, ensure_ascii=False, indent=2), encoding='utf-8')

    print('-' * 72)
    print('RECONSTRUCCION COMPLETADA')
    print(f"Anios:        {totals['anios']}")
    print(f"Nuevos:       {totals['nuevos']}")
    print(f"Saltados:     {totals['saltados']}")
    print(f"Errores:      {totals['errores']}")
    print(f"Items vistos: {totals['total_items']}")
    print(f'Reporte JSON: {report_path}')


if __name__ == '__main__':
    main()


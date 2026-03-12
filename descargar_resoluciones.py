"""
SII Normativa — Descargador de Resoluciones Exentas
====================================================
Como no hay índice HTML público, probamos URLs directas:
  https://www.sii.cl/normativa_legislacion/resoluciones/{año}/reso{num}.pdf

Uso:
  python descargar_resoluciones.py                    # Solo 2025
  python descargar_resoluciones.py --desde 2015 --hasta 2025
  python descargar_resoluciones.py --desde 2020 --hasta 2025 --max 200
"""

import os, sys, time, argparse
from datetime import date

# Agregar carpeta scraper al path para importar engine
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE, 'scraper'))

from engine import (
    SESSION, descargar_pdf, extraer_texto_pdf, doc_existe, doc_existe_hash,
    detectar_leyes, detectar_articulos, extraer_fecha_texto, extraer_resumen,
    guardar_documento, log_scraper, log, PDF_DIR
)
import hashlib, json
from pdf_layout import build_pdf_path

SII_BASE = "https://www.sii.cl/normativa_legislacion"

def descargar_resoluciones(anio, max_num=300, delay=0.5):
    """Descarga resoluciones de un año probando del 1 al max_num."""
    
    print(f"\n{'='*60}")
    print(f"  RESOLUCIONES {anio} — probando del 1 al {max_num}")
    print(f"{'='*60}")
    
    nuevos = 0
    existentes = 0
    no_encontrados = 0
    errores = 0
    consecutivos_404 = 0  # Para parar si ya no hay más
    
    for num in range(1, max_num + 1):
        numero = str(num)
        
        # ¿Ya la tenemos?
        if doc_existe('resolucion', numero, anio):
            existentes += 1
            if existentes % 20 == 0:
                print(f"  [{num}/{max_num}] Ya indexadas: {existentes}")
            consecutivos_404 = 0
            continue
        
        # Intentar descargar
        url = f"{SII_BASE}/resoluciones/{anio}/reso{num}.pdf"
        pdf_bytes = descargar_pdf(url, retries=2, delay=0.3)
        
        if pdf_bytes is None:
            no_encontrados += 1
            consecutivos_404 += 1
            # Si llevamos 30 seguidos sin encontrar, probablemente ya no hay más
            if consecutivos_404 >= 30:
                print(f"  [{num}] 30 consecutivos sin PDF — parando año {anio}")
                break
            continue
        
        consecutivos_404 = 0
        
        # Guardar PDF en disco
        pdf_path = build_pdf_path("resolucion", anio, f"resolucion_{anio}_{numero.zfill(4)}.pdf")
        try:
            with open(pdf_path, 'wb') as f:
                f.write(pdf_bytes)
        except:
            pass
        
        # Extraer texto
        ext = extraer_texto_pdf(pdf_bytes)
        if not ext['ok'] or ext['chars'] < 50:
            errores += 1
            log_scraper('resolucion', anio, numero, 'extraccion_fallida', url)
            continue
        
        texto = ext['texto']
        hash_doc = hashlib.md5(pdf_bytes).hexdigest()
        
        if doc_existe_hash(hash_doc):
            existentes += 1
            continue
        
        # Analizar
        leyes = detectar_leyes(texto)
        arts = detectar_articulos(texto)
        fecha = extraer_fecha_texto(texto) or f"{anio}-01-01"
        resumen = extraer_resumen(texto)
        
        # Título: intentar extraer del texto
        titulo = f"Resolución Exenta SII N°{numero} de {anio}"
        for linea in texto.split('\n')[:20]:
            linea = linea.strip()
            if len(linea) > 20 and len(linea) < 200 and linea.isupper():
                titulo = linea[:300]
                break
        
        doc_data = {
            'hash_md5': hash_doc,
            'tipo': 'resolucion',
            'numero': numero,
            'anio': anio,
            'fecha': fecha,
            'titulo': titulo[:500],
            'materia': None,
            'subtema': '',
            'contenido': texto[:50000],
            'resumen': resumen,
            'url_sii': url,
            'referencia': f"Resolución Ex. SII N°{numero} de {anio}",
            'palabras_clave': None,
            'leyes_citadas': json.dumps(leyes),
            'articulos_clave': json.dumps(arts[:20]),
            'fuente': 'scraper',
        }
        
        doc_id = guardar_documento(doc_data)
        if doc_id:
            nuevos += 1
            log_scraper('resolucion', anio, numero, 'ok', url)
            print(f"  ✅ [{num}/{max_num}] Res. Ex. N°{numero}/{anio} — {ext['paginas']} págs, {len(leyes)} leyes")
        else:
            errores += 1
        
        time.sleep(delay)
    
    print(f"\n  RESULTADO {anio}:")
    print(f"  ✅ Nuevas:       {nuevos}")
    print(f"  ⏭  Ya existían:  {existentes}")
    print(f"  ❌ No encontradas: {no_encontrados}")
    print(f"  ⚠  Errores:      {errores}")
    
    return {'anio': anio, 'nuevos': nuevos, 'existentes': existentes, 
            'no_encontrados': no_encontrados, 'errores': errores}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Descargar Resoluciones SII')
    parser.add_argument('--desde', type=int, default=date.today().year,
                        help='Año desde (default: año actual)')
    parser.add_argument('--hasta', type=int, default=date.today().year,
                        help='Año hasta (default: año actual)')
    parser.add_argument('--max', type=int, default=300,
                        help='Número máximo a probar por año (default: 300)')
    parser.add_argument('--delay', type=float, default=0.5,
                        help='Segundos entre descargas (default: 0.5)')
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("  DESCARGADOR DE RESOLUCIONES SII")
    print(f"  Años: {args.desde} a {args.hasta}")
    print(f"  Rango: 1 a {args.max} por año")
    print("="*60)
    
    resultados = []
    for anio in range(args.hasta, args.desde - 1, -1):
        r = descargar_resoluciones(anio, max_num=args.max, delay=args.delay)
        resultados.append(r)
        time.sleep(2)
    
    # Resumen final
    total_nuevos = sum(r['nuevos'] for r in resultados)
    print("\n" + "="*60)
    print(f"  TOTAL: {total_nuevos} resoluciones nuevas descargadas")
    print("="*60)


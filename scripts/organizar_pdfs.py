import os
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PDF_ROOT = ROOT / 'pdfs'

MODERN_RE = re.compile(r'^(circular|oficio|resolucion)_(\d{4})_.+\.pdf$', re.I)
HIST_RE = re.compile(r'^circular_historica_(\d{4})_.+\.pdf$', re.I)
JUDICIAL_RE = re.compile(r'^judicial_(\d{4})_.+\.pdf$', re.I)


def classify_target(path: Path):
    rel_parts = path.relative_to(PDF_ROOT).parts
    name = path.name

    if len(rel_parts) >= 3 and rel_parts[0].lower() in {'circular', 'oficio', 'resolucion', 'judicial'}:
        return None

    m = MODERN_RE.match(name)
    if m:
        tipo, anio = m.groups()
        return PDF_ROOT / tipo.lower() / anio / name

    m = HIST_RE.match(name)
    if m:
        anio = m.group(1)
        return PDF_ROOT / 'circular' / anio / name

    m = JUDICIAL_RE.match(name)
    if m:
        anio = m.group(1)
        return PDF_ROOT / 'judicial' / anio / name

    return None


def move_all():
    moves = []
    skipped = []
    for path in PDF_ROOT.rglob('*.pdf'):
        if not path.is_file():
            continue
        target = classify_target(path)
        if target is None:
            skipped.append(path)
            continue
        if path.resolve() == target.resolve():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if target.stat().st_size == path.stat().st_size:
                path.unlink()
                continue
            raise RuntimeError(f'Conflicto de destino: {path} -> {target}')
        shutil.move(str(path), str(target))
        moves.append((path, target))

    for folder in sorted(PDF_ROOT.rglob('*'), key=lambda p: len(p.parts), reverse=True):
        if folder.is_dir() and folder != PDF_ROOT:
            try:
                next(folder.iterdir())
            except StopIteration:
                folder.rmdir()

    print('=' * 72)
    print('  ORGANIZAR PDFS')
    print('=' * 72)
    print(f'Movidos:   {len(moves)}')
    print(f'Skip/otros:{len(skipped)}')
    for src, dst in moves[:20]:
        print(f'{src.relative_to(ROOT)} -> {dst.relative_to(ROOT)}')
    if len(moves) > 20:
        print(f'... {len(moves) - 20} movimientos mas')


if __name__ == '__main__':
    move_all()

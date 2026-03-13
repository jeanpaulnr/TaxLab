import json
import re
import unicodedata

CANONICAL_BODIES = (
    'CT',
    'LIR',
    'LIVS',
    'ITE',
    'LHD',
    'LIT',
    'FT',
    'OCN',
    'CI',
)

BODY_PATTERNS = {
    'CT': [
        r'c[oó]digo\s+tributario',
        r'\bct\b',
        r'd\.?\s*l\.?\s*830\b',
    ],
    'LIR': [
        r'ley\s+sobre\s+impuesto\s+a\s+la\s+renta',
        r'\blir\b',
        r'd\.?\s*l\.?\s*824\b',
    ],
    'LIVS': [
        r'ley\s+sobre\s+impuesto\s+a\s+las\s+ventas\s+y\s+servicios',
        r'\blivs\b',
        r'd\.?\s*l\.?\s*825\b',
    ],
    'ITE': [
        r'ley\s+sobre\s+impuesto\s+de\s+timbres\s+y\s+estampillas',
        r'\bite\b',
        r'decreto\s+ley\s+n?[°ºo]?\s*3\.?475\b',
        r'd\.?\s*l\.?\s*3\.?475\b',
    ],
    'LHD': [
        r'ley\s+sobre\s+impuesto\s+a\s+las?\s+herencias(?:,\s*asignaciones\s+y\s+donaciones)?',
        r'\blhd\b',
        r'ley\s+n?[°ºo]?\s*16\.?271\b',
    ],
    'LIT': [
        r'ley\s+sobre\s+impuesto\s+territorial',
        r'\blit\b',
        r'impuesto\s+territorial',
    ],
    'FT': [
        r'franquicias\s+tributarias',
        r'\bft\b',
    ],
    'OCN': [
        r'otros?\s+cuerpos?\s+normativos',
        r'\bocn\b',
        r'otras?\s+normas',
    ],
    'CI': [
        r'convenios?\s+internacionales',
        r'\bci\b',
        r'convenio\s+para\s+evitar',
    ],
}

BODY_LABELS = {
    'CT': 'Codigo Tributario',
    'LIR': 'Ley sobre Impuesto a la Renta',
    'LIVS': 'Ley sobre Impuesto a las Ventas y Servicios',
    'ITE': 'Ley sobre Impuesto de Timbres y Estampillas',
    'LHD': 'Ley sobre Impuesto a las Herencias, Asignaciones y Donaciones',
    'LIT': 'Ley sobre Impuesto Territorial',
    'FT': 'Franquicias Tributarias',
    'OCN': 'Otros Cuerpos Normativos',
    'CI': 'Convenios Internacionales',
}

BODY_REGEX = {
    code: '(?:' + '|'.join(patterns) + ')'
    for code, patterns in BODY_PATTERNS.items()
}

EXPLICIT_ARTICLE_RE = re.compile(
    r'(?:art(?:[ií]culo)?s?\.?)\s*'
    r'(?P<art>\d{1,3}(?:\s*(?:bis|ter|quater|quater|qu[aá]ter))?'
    r'(?:\s+inciso\s+(?:primero|segundo|tercero|cuarto|quinto|sexto|final))?'
    r'(?:\s+letra\s+[a-z])?'
    r'(?:\s+n[°ºo]?\s*\d+)?)',
    re.IGNORECASE,
)

LEADING_ARTICLE_RE = re.compile(
    r'^\s*(?P<art>\d{1,3}(?:\s*(?:bis|ter|quater|quater|qu[aá]ter))?'
    r'(?:\s+inciso\s+(?:primero|segundo|tercero|cuarto|quinto|sexto|final))?'
    r'(?:\s+letra\s+[a-z])?'
    r'(?:\s+n[°ºo]?\s*\d+)?)',
    re.IGNORECASE,
)


def _slugify(value):
    normalized = unicodedata.normalize('NFKD', str(value or '')).encode('ascii', 'ignore').decode('ascii')
    normalized = re.sub(r'[^a-zA-Z0-9]+', '-', normalized.lower()).strip('-')
    return normalized or 'sin-slug'


def normalize_body_code(value):
    if not value:
        return None
    text = str(value).strip().upper()
    aliases = {
        'LTE': 'ITE',
        'LH': 'LHD',
        'LMT': 'OCN',
        'LRT': 'OCN',
        'OTRAS': 'OCN',
        'OTROS': 'OCN',
    }
    text = aliases.get(text, text)
    return text if text in CANONICAL_BODIES else None


def normalize_article_value(value):
    if not value:
        return ''
    text = str(value).strip()
    text = re.sub(r'^(?:art(?:[ií]culo)?s?\.?)\s*', '', text, flags=re.IGNORECASE)
    text = text.lower()
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\s+n[°ºo]?\s*', ' N° ', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+inciso\s+', ' inciso ', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+letra\s+', ' letra ', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+', ' ', text).strip(' ,;:.')
    return text


def build_article_ref(cuerpo, articulo, ambiguous=False):
    articulo_norm = normalize_article_value(articulo)
    cuerpo_norm = normalize_body_code(cuerpo)
    if ambiguous or not cuerpo_norm:
        return {
            'cuerpo': None,
            'articulo': articulo_norm,
            'clave': None,
            'label': f'Art. {articulo_norm} (ambiguo)' if articulo_norm else 'Articulo ambiguo',
            'slug': f"art-{_slugify(articulo_norm or 'ambiguo')}",
            'ambigua': True,
        }
    return {
        'cuerpo': cuerpo_norm,
        'articulo': articulo_norm,
        'clave': f'{cuerpo_norm}:{articulo_norm}',
        'label': f'Art. {articulo_norm} {cuerpo_norm}',
        'slug': f"{cuerpo_norm.lower()}-art-{_slugify(articulo_norm)}",
        'ambigua': False,
    }


def parse_article_ref_list(value, include_ambiguous=True):
    if not value:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = [value]
    refs = []
    seen = set()
    for item in value or []:
        if isinstance(item, dict):
            ref = build_article_ref(item.get('cuerpo'), item.get('articulo'), bool(item.get('ambigua')))
        else:
            ref = build_article_ref(None, item, ambiguous=True)
        if ref['ambigua'] and not include_ambiguous:
            continue
        key = ref['clave'] or f"AMB:{ref['articulo']}"
        if key in seen:
            continue
        seen.add(key)
        refs.append(ref)
    return refs


def detect_normative_bodies(text):
    haystack = (text or '').lower()
    found = []
    for code in CANONICAL_BODIES:
        if any(re.search(pattern, haystack, re.IGNORECASE) for pattern in BODY_PATTERNS[code]):
            found.append(code)
    return found


def _parse_article_blob(blob):
    blob = re.sub(r'\s+', ' ', blob or '').strip(' ,;:.')
    refs = []
    seen = set()

    leading = LEADING_ARTICLE_RE.search(blob)
    if leading:
        article = normalize_article_value(leading.group('art'))
        if article and article not in seen:
            refs.append(article)
            seen.add(article)

    for match in EXPLICIT_ARTICLE_RE.finditer(blob):
        article = normalize_article_value(match.group('art'))
        if article and article not in seen:
            refs.append(article)
            seen.add(article)

    simple_parts = [part.strip() for part in re.split(r'\b(?:y|e)\b|,|;', blob) if part.strip()]
    if simple_parts and all(re.fullmatch(r'\d{1,3}', part) for part in simple_parts):
        for part in simple_parts:
            if part not in seen:
                refs.append(part)
                seen.add(part)

    return refs


def detect_normative_references(text):
    text = text or ''
    refs = []
    seen_refs = set()

    body_spans = []
    for body_code, patterns in BODY_PATTERNS.items():
        for pattern in patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                span = {
                    'cuerpo': body_code,
                    'start': match.start(),
                    'end': match.end(),
                }
                if span not in body_spans:
                    body_spans.append(span)
    body_spans.sort(key=lambda item: (item['start'], item['end']))

    def segment_bounds(position):
        left = max(
            text.rfind('\n', 0, position),
            text.rfind(';', 0, position),
            text.rfind(':', 0, position),
        )
        right_candidates = [
            idx
            for idx in (
                text.find('\n', position),
                text.find(';', position),
                text.find(':', position),
            )
            if idx >= 0
        ]
        right = min(right_candidates) if right_candidates else len(text)
        return left + 1, right

    def forward_body_candidate(article_match, next_article_start, seg_end):
        candidates = [
            body for body in body_spans
            if article_match.end() <= body['start'] < min(next_article_start, seg_end)
        ]
        if not candidates:
            return None
        nearest = candidates[0]
        bridge = text[article_match.end():nearest['start']]
        if re.search(r'\bde(?:l| la| las| los)?\b', bridge, re.IGNORECASE):
            return nearest
        return None

    article_matches = list(EXPLICIT_ARTICLE_RE.finditer(text))
    ambiguous_seen = set()
    for index, match in enumerate(article_matches):
        article = normalize_article_value(match.group('art'))
        if not article:
            continue
        seg_start, seg_end = segment_bounds(match.start())
        next_start = article_matches[index + 1].start() if index + 1 < len(article_matches) else seg_end

        candidates_before = [
            body for body in body_spans
            if body['start'] >= seg_start and body['end'] <= match.start()
        ]
        following = forward_body_candidate(match, next_start, seg_end)
        if following:
            ref = build_article_ref(following['cuerpo'], article)
        elif candidates_before:
            ref = build_article_ref(candidates_before[-1]['cuerpo'], article)
        else:
            ref = build_article_ref(None, article, ambiguous=True)

        key = ref['clave'] or f"AMB:{ref['articulo']}"
        if key in seen_refs or key in ambiguous_seen:
            continue
        if ref.get('ambigua'):
            ambiguous_seen.add(key)
        else:
            seen_refs.add(key)
        refs.append(ref)
    return refs


def serialize_article_refs(refs, include_ambiguous=True):
    items = parse_article_ref_list(refs, include_ambiguous=include_ambiguous)
    return json.dumps(items, ensure_ascii=False)


def exact_article_refs(refs):
    return [ref for ref in parse_article_ref_list(refs) if not ref.get('ambigua')]


def article_labels(refs, include_ambiguous=False):
    return [ref['label'] for ref in parse_article_ref_list(refs, include_ambiguous=include_ambiguous)]

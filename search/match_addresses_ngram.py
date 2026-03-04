#!/usr/bin/env python3
import json
import re
from typing import Dict, Any, List, Tuple

from rapidfuzz.distance import Levenshtein

INPUT_ACCT = 'df_acct_addr_coordinates_small.json'
INPUT_BX24 = 'df_bx24_location_geodata_v2_small.json'
OUTPUT_BX24 = 'df_bx24_location_geodata_v2_small_matched.json'
OUTPUT_ACCT = 'df_acct_addr_coordinates_small_matched.json'


def load_columnar_json(path: str) -> Dict[str, Dict[str, Any]]:
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f'{path}: expected dict, got {type(data).__name__}')
    return data


def columnar_to_rows(data: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Collect all row indices across columns
    idx = set()
    for col, mapping in data.items():
        if isinstance(mapping, dict):
            idx.update(mapping.keys())
    rows = []
    for i in sorted(idx, key=lambda x: int(x) if str(x).isdigit() else str(x)):
        row = {}
        for col, mapping in data.items():
            row[col] = mapping.get(i) if isinstance(mapping, dict) else None
        row['__idx__'] = i
        rows.append(row)
    return rows


_TOKEN_MAP = {
    'ул': 'улица',
    'улиц': 'улица',
    'пр': 'проспект',
    'просп': 'проспект',
    'пер': 'переулок',
    'переул': 'переулок',
    'д': 'дом',
    'кв': 'квартира',
    'корп': 'корпус',
    'стр': 'строение',
    'оф': 'офис',
    'ш': 'шоссе',
    'пл': 'площадь',
    'наб': 'набережная',
    'б': 'бульвар',
    'мкр': 'микрорайон',
    'рн': 'район',
    'г': 'город',
    'пос': 'поселок',
    'ст': 'станция',
    'тер': 'территория',
}


def normalize_address(text: Any) -> str:
    if text is None:
        return ''
    text = str(text).lower()
    text = re.sub(r'[\t\r\n]+', ' ', text)
    text = text.replace('\u00a0', ' ')
    text = re.sub(r'[^\w\s]', ' ', text, flags=re.UNICODE)
    tokens = [t for t in text.split() if t]
    tokens = [_TOKEN_MAP.get(t, t) for t in tokens]
    return ' '.join(tokens)


def rows_to_columnar(rows: List[Dict[str, Any]], existing_cols: List[str]) -> Dict[str, Dict[str, Any]]:
    cols: Dict[str, Dict[str, Any]] = {c: {} for c in existing_cols}
    for row in rows:
        i = row.get('__idx__')
        for c in existing_cols:
            cols[c][i] = row.get(c)
    return cols


def _ngrams(s: str, n: int = 3) -> List[str]:
    if len(s) <= n:
        return [s] if s else []
    return [s[i:i + n] for i in range(len(s) - n + 1)]


def _ngram_similarity(a: str, b: str, n: int = 3) -> float:
    if not a or not b:
        return 0.0
    a_ngrams = _ngrams(a, n)
    b_ngrams = _ngrams(b, n)
    if not a_ngrams or not b_ngrams:
        return 0.0
    a_set = set(a_ngrams)
    b_set = set(b_ngrams)
    inter = len(a_set & b_set)
    union = len(a_set | b_set)
    return (inter / union) * 100.0 if union else 0.0


def best_match(target: str, candidates: List[Tuple[str, Dict[str, Any]]]) -> Tuple[Dict[str, Any], int, float]:
    best_row = None
    best_sim = -1.0
    for addr, row in candidates:
        if not addr:
            continue
        sim = _ngram_similarity(target, addr, n=3)
        if sim > best_sim:
            best_sim = sim
            best_row = row
    return best_row, int(best_sim) if best_sim >= 0 else -1, best_sim


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # Returns distance in meters.
    import math
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def main() -> None:
    acct = load_columnar_json(INPUT_ACCT)
    bx24 = load_columnar_json(INPUT_BX24)

    acct_rows = columnar_to_rows(acct)
    bx24_rows = columnar_to_rows(bx24)

    # Prebuild candidates list
    bx24_candidates = [(normalize_address(r.get('actual_address') or ''), r) for r in bx24_rows]

    # Ensure output columns exist in bx24 columnar
    extra_cols = [
        'match_id',
        'match_actual_address',
        'match_lat_o',
        'match_lng_o',
        'match_score',
        'match_score_norm',
        'match_threshold',
        'match_status',
    ]
    for c in extra_cols:
        if c not in bx24:
            bx24[c] = {}

    # For each row in acct, find best match in bx24 by actual_address
    # Then write matched acct fields into the matched bx24 row (one best row per acct row)
    # If multiple acct rows map to same bx24 row, last one wins.
    bx24_by_idx = {r['__idx__']: r for r in bx24_rows}

    # Prepare acct output columns
    acct_extra_cols = [
        'match_bx24_id',
        'match_actual_address',
        'match_score',
        'match_score_norm',
        'match_threshold',
        'match_status',
        'match_distance_m',
    ]
    for c in acct_extra_cols:
        if c not in acct:
            acct[c] = {}

    for arow in acct_rows:
        target = normalize_address(arow.get('actual_address') or '')
        if not target:
            continue
        brow, score, sim = best_match(target, bx24_candidates)
        if not brow:
            continue
        # Determine threshold bucket
        if sim >= 90.0:
            threshold = 90
            status = 'strong'
        elif sim >= 80.0:
            threshold = 80
            status = 'weak'
        else:
            threshold = 0
            status = 'rejected'
        idx = brow['__idx__']
        if status != 'rejected':
            # add data from acct row into matched bx24 row
            brow['match_id'] = arow.get('id')
            brow['match_actual_address'] = arow.get('actual_address')
            brow['match_lat_o'] = arow.get('lat')
            brow['match_lng_o'] = arow.get('lng')
            brow['match_score'] = score
            brow['match_score_norm'] = sim
            brow['match_threshold'] = threshold
            brow['match_status'] = status
            bx24_by_idx[idx] = brow

        # Add match info to acct row
        arow['match_bx24_id'] = brow.get('id') if status != 'rejected' else None
        arow['match_actual_address'] = brow.get('actual_address') if status != 'rejected' else None
        arow['match_score'] = score
        arow['match_score_norm'] = sim
        arow['match_threshold'] = threshold
        arow['match_status'] = status
        # Distance between acct(lat,lng) and bx24(lat_o,lng_o)
        try:
            lat1 = float(arow.get('lat'))
            lon1 = float(arow.get('lng'))
            if status != 'rejected':
                lat2 = float(brow.get('lat_o'))
                lon2 = float(brow.get('lng_o'))
                arow['match_distance_m'] = haversine_meters(lat1, lon1, lat2, lon2)
            else:
                arow['match_distance_m'] = None
        except (TypeError, ValueError):
            arow['match_distance_m'] = None

    # Rebuild columnar output using existing bx24 columns + new ones
    out_bx24_cols = list(bx24.keys())
    out_bx24_rows = list(bx24_by_idx.values())
    out_bx24 = rows_to_columnar(out_bx24_rows, out_bx24_cols)

    out_acct_cols = list(acct.keys())
    out_acct_rows = acct_rows
    out_acct = rows_to_columnar(out_acct_rows, out_acct_cols)

    with open(OUTPUT_BX24, 'w', encoding='utf-8') as f:
        json.dump(out_bx24, f, ensure_ascii=False)

    with open(OUTPUT_ACCT, 'w', encoding='utf-8') as f:
        json.dump(out_acct, f, ensure_ascii=False)


if __name__ == '__main__':
    main()

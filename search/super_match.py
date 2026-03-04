#!/usr/bin/env python3
import argparse
import json
import re
from typing import Dict, Any, List, Tuple

from rapidfuzz.distance import Levenshtein
from rapidfuzz import fuzz

DEFAULT_INPUT_ACCT = 'df_acct_addr_coordinates_small.json'
DEFAULT_INPUT_BX24 = 'df_bx24_location_geodata_v2_small.json'
DEFAULT_OUTPUT_ACCT = 'df_acct_addr_coordinates_small_super_matched.json'


def load_columnar_json(path: str) -> Dict[str, Dict[str, Any]]:
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f'{path}: expected dict, got {type(data).__name__}')
    return data


def columnar_to_rows(data: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    idx = set()
    for col, mapping in data.items():
        if isinstance(mapping, dict):
            idx.update(mapping.keys())
    rows = []
    for i in sorted(idx, key=lambda x: int(x) if str(x).isdigit() else str(x)):
        row = {col: (mapping.get(i) if isinstance(mapping, dict) else None) for col, mapping in data.items()}
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


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math
    r = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def best_match_levenshtein(target: str, candidates: List[Tuple[str, Dict[str, Any]]]) -> Tuple[Dict[str, Any], int, float]:
    best_row = None
    best_score = None
    best_sim = -1.0
    for addr, row in candidates:
        if not addr:
            continue
        score = Levenshtein.distance(target, addr)
        max_len = max(len(target), len(addr))
        sim = (1.0 - (score / max_len)) * 100.0 if max_len > 0 else 0.0
        if sim > best_sim or (sim == best_sim and (best_score is None or score < best_score)):
            best_sim = sim
            best_score = score
            best_row = row
    return best_row, best_score if best_score is not None else -1, best_sim


def best_match_token_set(target: str, candidates: List[Tuple[str, Dict[str, Any]]]) -> Tuple[Dict[str, Any], int, float]:
    best_row = None
    best_sim = -1.0
    for addr, row in candidates:
        if not addr:
            continue
        sim = float(fuzz.token_set_ratio(target, addr))
        if sim > best_sim:
            best_sim = sim
            best_row = row
    return best_row, int(best_sim) if best_sim >= 0 else -1, best_sim


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


def best_match_ngram(target: str, candidates: List[Tuple[str, Dict[str, Any]]]) -> Tuple[Dict[str, Any], int, float]:
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


def classify(sim: float) -> Tuple[int, str]:
    if sim >= 90.0:
        return 90, 'strong'
    if sim >= 80.0:
        return 80, 'weak'
    return 0, 'rejected'


def add_fields(row: Dict[str, Any], prefix: str, bx24_row: Dict[str, Any], score: int, sim: float, dist: float) -> None:
    threshold, status = classify(sim)
    row[f'{prefix}match_bx24_id'] = bx24_row.get('id') if status != 'rejected' else None
    row[f'{prefix}match_actual_address'] = bx24_row.get('actual_address') if status != 'rejected' else None
    row[f'{prefix}match_score'] = score
    row[f'{prefix}match_score_norm'] = sim
    row[f'{prefix}match_threshold'] = threshold
    row[f'{prefix}match_status'] = status
    row[f'{prefix}match_distance_m'] = dist if status != 'rejected' else None


def main() -> None:
    parser = argparse.ArgumentParser(description='Run 4 address matching strategies and merge into one output file.')
    parser.add_argument('--acct', default=DEFAULT_INPUT_ACCT, help='Input acct JSON (columnar).')
    parser.add_argument('--bx24', default=DEFAULT_INPUT_BX24, help='Input bx24 JSON (columnar).')
    parser.add_argument('--out', default=DEFAULT_OUTPUT_ACCT, help='Output acct JSON (columnar).')
    args = parser.parse_args()

    acct = load_columnar_json(args.acct)
    bx24 = load_columnar_json(args.bx24)

    acct_rows = columnar_to_rows(acct)
    bx24_rows = columnar_to_rows(bx24)

    # Candidate lists
    bx24_actual = [(normalize_address(r.get('actual_address') or ''), r) for r in bx24_rows]
    bx24_ya = [(normalize_address(r.get('ya_reverse') or ''), r) for r in bx24_rows]

    # Ensure output columns exist in acct
    prefixes = ['lv_act_', 'lv_ya_', 'ts_act_', 'ts_ya_', 'ng_act_', 'ng_ya_']
    base_cols = ['match_bx24_id', 'match_actual_address', 'match_score', 'match_score_norm', 'match_threshold', 'match_status', 'match_distance_m']
    for p in prefixes:
        for c in base_cols:
            col = f'{p}{c}'
            if col not in acct:
                acct[col] = {}

    for arow in acct_rows:
        target = normalize_address(arow.get('actual_address') or '')
        if not target:
            continue

        # 1) Levenshtein vs actual_address, distance vs bx24 lat_o/lng_o
        brow, score, sim = best_match_levenshtein(target, bx24_actual)
        if brow:
            dist = None
            try:
                lat1 = float(arow.get('lat'))
                lon1 = float(arow.get('lng'))
                lat2 = float(brow.get('lat_o'))
                lon2 = float(brow.get('lng_o'))
                dist = haversine_meters(lat1, lon1, lat2, lon2)
            except (TypeError, ValueError):
                dist = None
            add_fields(arow, 'lv_act_', brow, score, sim, dist)

        # 2) Levenshtein vs ya_reverse, distance vs bx24 lat/lng
        brow, score, sim = best_match_levenshtein(target, bx24_ya)
        if brow:
            dist = None
            try:
                lat1 = float(arow.get('lat'))
                lon1 = float(arow.get('lng'))
                lat2 = float(brow.get('lat'))
                lon2 = float(brow.get('lng'))
                dist = haversine_meters(lat1, lon1, lat2, lon2)
            except (TypeError, ValueError):
                dist = None
            add_fields(arow, 'lv_ya_', brow, score, sim, dist)

        # 3) token_set_ratio vs actual_address, distance vs bx24 lat_o/lng_o
        brow, score, sim = best_match_token_set(target, bx24_actual)
        if brow:
            dist = None
            try:
                lat1 = float(arow.get('lat'))
                lon1 = float(arow.get('lng'))
                lat2 = float(brow.get('lat_o'))
                lon2 = float(brow.get('lng_o'))
                dist = haversine_meters(lat1, lon1, lat2, lon2)
            except (TypeError, ValueError):
                dist = None
            add_fields(arow, 'ts_act_', brow, score, sim, dist)

        # 4) token_set_ratio vs ya_reverse, distance vs bx24 lat/lng
        brow, score, sim = best_match_token_set(target, bx24_ya)
        if brow:
            dist = None
            try:
                lat1 = float(arow.get('lat'))
                lon1 = float(arow.get('lng'))
                lat2 = float(brow.get('lat'))
                lon2 = float(brow.get('lng'))
                dist = haversine_meters(lat1, lon1, lat2, lon2)
            except (TypeError, ValueError):
                dist = None
            add_fields(arow, 'ts_ya_', brow, score, sim, dist)

        # 5) n-gram vs actual_address, distance vs bx24 lat_o/lng_o
        brow, score, sim = best_match_ngram(target, bx24_actual)
        if brow:
            dist = None
            try:
                lat1 = float(arow.get('lat'))
                lon1 = float(arow.get('lng'))
                lat2 = float(brow.get('lat_o'))
                lon2 = float(brow.get('lng_o'))
                dist = haversine_meters(lat1, lon1, lat2, lon2)
            except (TypeError, ValueError):
                dist = None
            add_fields(arow, 'ng_act_', brow, score, sim, dist)

        # 6) n-gram vs ya_reverse, distance vs bx24 lat/lng
        brow, score, sim = best_match_ngram(target, bx24_ya)
        if brow:
            dist = None
            try:
                lat1 = float(arow.get('lat'))
                lon1 = float(arow.get('lng'))
                lat2 = float(brow.get('lat'))
                lon2 = float(brow.get('lng'))
                dist = haversine_meters(lat1, lon1, lat2, lon2)
            except (TypeError, ValueError):
                dist = None
            add_fields(arow, 'ng_ya_', brow, score, sim, dist)

    out_cols = list(acct.keys())
    out = rows_to_columnar(acct_rows, out_cols)

    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False)


if __name__ == '__main__':
    main()

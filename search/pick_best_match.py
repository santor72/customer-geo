#!/usr/bin/env python3
import argparse
import json
from typing import Dict, Any, List


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


def rows_to_columnar(rows: List[Dict[str, Any]], existing_cols: List[str]) -> Dict[str, Dict[str, Any]]:
    cols: Dict[str, Dict[str, Any]] = {c: {} for c in existing_cols}
    for row in rows:
        i = row.get('__idx__')
        for c in existing_cols:
            cols[c][i] = row.get(c)
    return cols


def _num(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return float('-inf')


def main() -> None:
    parser = argparse.ArgumentParser(description='Pick best match from super_match output.')
    parser.add_argument('--in', dest='inp', default='df_acct_addr_coordinates_small_super_matched.json',
                        help='Input super-matched acct JSON (columnar).')
    parser.add_argument('--out', dest='out', default='df_acct_addr_coordinates_best_matched.json',
                        help='Output acct JSON (columnar) with best match fields.')
    args = parser.parse_args()

    data = load_columnar_json(args.inp)
    rows = columnar_to_rows(data)

    prefixes = ['lv_act_', 'lv_ya_', 'ts_act_', 'ts_ya_', 'ng_act_', 'ng_ya_']

    # Output fields requested (note: keeping exact names from request)
    out_fields = [
        'best_match_bx24_id',
        'best_match_actual_address',
        'best_match_score',
        'best_act_match_score_norm',
        'best_act_match_threshold',
        'best_act_match_status',
        'best_act_match_distance_m',
        'best_match_algo',
    ]
    for f in out_fields:
        if f not in data:
            data[f] = {}

    for row in rows:
        best_prefix = None
        best_score = float('-inf')
        best_dist = float('inf')

        for p in prefixes:
            status = row.get(f'{p}match_status')
            if status == 'rejected' or status is None:
                continue
            score_norm = _num(row.get(f'{p}match_score_norm'))
            dist = row.get(f'{p}match_distance_m')
            dist_val = _num(dist) if dist is not None else float('inf')

            if score_norm > best_score or (score_norm == best_score and dist_val < best_dist):
                best_score = score_norm
                best_dist = dist_val
                best_prefix = p

        if best_prefix is None:
            row['best_match_bx24_id'] = None
            row['best_match_actual_address'] = None
            row['best_match_score'] = None
            row['best_act_match_score_norm'] = None
            row['best_act_match_threshold'] = None
            row['best_act_match_status'] = 'rejected'
            row['best_act_match_distance_m'] = None
            row['best_match_algo'] = None
            continue

        row['best_match_bx24_id'] = row.get(f'{best_prefix}match_bx24_id')
        row['best_match_actual_address'] = row.get(f'{best_prefix}match_actual_address')
        row['best_match_score'] = row.get(f'{best_prefix}match_score')
        row['best_act_match_score_norm'] = row.get(f'{best_prefix}match_score_norm')
        row['best_act_match_threshold'] = row.get(f'{best_prefix}match_threshold')
        row['best_act_match_status'] = row.get(f'{best_prefix}match_status')
        row['best_act_match_distance_m'] = row.get(f'{best_prefix}match_distance_m')
        row['best_match_algo'] = best_prefix.rstrip('_')

    out_cols = list(data.keys())
    out = rows_to_columnar(rows, out_cols)

    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False)


if __name__ == '__main__':
    main()

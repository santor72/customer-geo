from typing import List, Tuple

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from .config import MYSQL_SCHEMA
from .db import get_engine, get_latest_recv_mon

try:
    import h3
except Exception:  # pragma: no cover
    h3 = None

app = FastAPI(title="customer-geo", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def parse_bbox(bbox: str) -> Tuple[float, float, float, float]:
    parts = [p.strip() for p in bbox.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must have 4 comma-separated numbers")
    try:
        min_lng, min_lat, max_lng, max_lat = map(float, parts)
    except ValueError as exc:
        raise ValueError("bbox values must be numbers") from exc
    if min_lng >= max_lng or min_lat >= max_lat:
        raise ValueError("bbox min must be less than max")
    if not (-180 <= min_lng <= 180 and -180 <= max_lng <= 180):
        raise ValueError("bbox lng out of range")
    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError("bbox lat out of range")
    return min_lng, min_lat, max_lng, max_lat


def require_current_month(month: str) -> str:
    if month != "current":
        raise HTTPException(status_code=400, detail="only month=current is supported in phase 1")
    return month


def table_name(base: str) -> str:
    if MYSQL_SCHEMA:
        return f"{MYSQL_SCHEMA}.{base}"
    return base


def h3_candidates(index_val):
    if h3 is None:
        raise HTTPException(status_code=500, detail="h3 library is not available")
    candidates = []
    if isinstance(index_val, str):
        candidates.append(index_val)
        try:
            candidates.append(format(int(index_val), "x"))
            candidates.append("0x" + format(int(index_val), "x"))
        except Exception:
            pass
    else:
        try:
            ival = int(index_val)
            candidates.append(ival)
            if hasattr(h3, "int_to_str"):
                candidates.append(h3.int_to_str(ival))
            candidates.append(format(ival, "x"))
            candidates.append("0x" + format(ival, "x"))
        except Exception:
            pass
    # de-dup while preserving order
    seen = set()
    uniq = []
    for c in candidates:
        key = str(c)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(c)
    return uniq


def h3_to_latlng(h3_idx: str):
    # h3 v4 uses cell_to_latlng, older versions use h3_to_geo
    if hasattr(h3, "cell_to_latlng"):
        return h3.cell_to_latlng(h3_idx)
    return h3.h3_to_geo(h3_idx)


def h3_to_boundary(h3_idx: str):
    # h3 v4 uses cell_to_boundary, older versions use h3_to_geo_boundary
    if hasattr(h3, "cell_to_boundary"):
        return h3.cell_to_boundary(h3_idx, geo_json=True)
    return h3.h3_to_geo_boundary(h3_idx, geo_json=True)


@app.get("/api/v1/months")
def get_months():
    return ["current"]


@app.get("/api/v1/layers/subscribers")
def get_subscribers(
    month: str = Query("current"),
    bbox: str = Query(..., description="minLng,minLat,maxLng,maxLat"),
    limit: int = Query(20000, ge=1, le=200000),
):
    require_current_month(month)
    try:
        min_lng, min_lat, max_lng, max_lat = parse_bbox(bbox)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    recv_mon = get_latest_recv_mon(table_name("agg_customer_cur"))
    sql = text(
        f"""
        SELECT account_id, settlement_id, lat, lng, is_active, is_sysblock, charges_sum_m, payments_sum_m
        FROM {table_name("agg_customer_cur")}
        WHERE recv_mon = :recv_mon
          AND lng BETWEEN :min_lng AND :max_lng
          AND lat BETWEEN :min_lat AND :max_lat
        LIMIT :limit
        """
    )
    df = pd.read_sql_query(
        sql,
        get_engine(),
        params={
            "recv_mon": recv_mon,
            "min_lng": min_lng,
            "max_lng": max_lng,
            "min_lat": min_lat,
            "max_lat": max_lat,
            "limit": limit,
        },
    )

    features = []
    for _, row in df.iterrows():
        if pd.isna(row["lat"]) or pd.isna(row["lng"]):
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(row["lng"]), float(row["lat"])],
                },
                "properties": {
                    "account_id": int(row["account_id"]),
                    "settlement_id": int(row["settlement_id"]) if pd.notna(row["settlement_id"]) else None,
                    "is_active": bool(row["is_active"]),
                    "is_sysblock": bool(row["is_sysblock"]),
                    "active_cnt": 1,
                    "charges_sum_m": float(row["charges_sum_m"]) if pd.notna(row["charges_sum_m"]) else 0.0,
                    "payments_sum_m": float(row["payments_sum_m"]) if pd.notna(row["payments_sum_m"]) else 0.0,
                },
            }
        )

    return {"type": "FeatureCollection", "features": features}


@app.get("/api/v1/layers/settlements")
def get_settlements(
    month: str = Query("current"),
    bbox: str = Query(..., description="minLng,minLat,maxLng,maxLat"),
):
    require_current_month(month)
    try:
        min_lng, min_lat, max_lng, max_lat = parse_bbox(bbox)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    recv_mon = get_latest_recv_mon(table_name("agg_settlement_cur"))
    sql = text(
        f"""
        SELECT settlement_id, lat, lng, active_cnt, charges_sum_m, payments_sum_m
        FROM {table_name("agg_settlement_cur")}
        WHERE recv_mon = :recv_mon
          AND lng BETWEEN :min_lng AND :max_lng
          AND lat BETWEEN :min_lat AND :max_lat
        """
    )
    df = pd.read_sql_query(
        sql,
        get_engine(),
        params={
            "recv_mon": recv_mon,
            "min_lng": min_lng,
            "max_lng": max_lng,
            "min_lat": min_lat,
            "max_lat": max_lat,
        },
    )

    features = []
    for _, row in df.iterrows():
        if pd.isna(row["lat"]) or pd.isna(row["lng"]):
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(row["lng"]), float(row["lat"])],
                },
                "properties": {
                    "settlement_id": int(row["settlement_id"]),
                    "active_cnt": int(row["active_cnt"]) if pd.notna(row["active_cnt"]) else 0,
                    "charges_sum_m": float(row["charges_sum_m"]) if pd.notna(row["charges_sum_m"]) else 0.0,
                    "payments_sum_m": float(row["payments_sum_m"]) if pd.notna(row["payments_sum_m"]) else 0.0,
                },
            }
        )

    return {"type": "FeatureCollection", "features": features}


@app.get("/api/v1/layers/h3")
def get_h3_layer(
    month: str = Query("current"),
    bbox: str = Query(..., description="minLng,minLat,maxLng,maxLat"),
    h3_res: int = Query(7, ge=0, le=15),
):
    require_current_month(month)
    try:
        min_lng, min_lat, max_lng, max_lat = parse_bbox(bbox)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    recv_mon = get_latest_recv_mon(table_name("agg_h3_cur"))
    sql = text(
        f"""
        SELECT h3_index, active_cnt, charges_sum_m, payments_sum_m
        FROM {table_name("agg_h3_cur")}
        WHERE recv_mon = :recv_mon
          AND h3_res = :h3_res
        """
    )
    df = pd.read_sql_query(
        sql,
        get_engine(),
        params={
            "recv_mon": recv_mon,
            "h3_res": h3_res,
        },
    )

    data = []
    for _, row in df.iterrows():
        try:
            h3_idx = (
                row["h3_index"]
                if isinstance(row["h3_index"], str)
                else format(int(row["h3_index"]), "x")
            )
        except Exception:
            continue
        if h3 is not None:
            try:
                center_lat, center_lng = h3_to_latlng(h3_idx)
                if not (min_lng <= center_lng <= max_lng and min_lat <= center_lat <= max_lat):
                    continue
            except Exception:
                # если не удалось вычислить центр — не фильтруем
                pass
        data.append(
            {
                "h3_index": h3_idx,
                "active_cnt": int(row["active_cnt"]) if pd.notna(row["active_cnt"]) else 0,
                "charges_sum_m": float(row["charges_sum_m"]) if pd.notna(row["charges_sum_m"]) else 0.0,
                "payments_sum_m": float(row["payments_sum_m"]) if pd.notna(row["payments_sum_m"]) else 0.0,
            }
        )

    return {"type": "H3", "data": data}

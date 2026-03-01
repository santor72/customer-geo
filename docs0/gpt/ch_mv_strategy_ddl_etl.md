# ClickHouse: MV strategy + DDL + ETL (staging → materialized views → marts)

This document bundles:
1) **Strategy** for staging + Materialized Views (MV) + marts  
2) **DDL** (tables + MVs)  
3) **ETL** templates:
   - SQL to populate `stg_account_month`
   - Python to generate `stg_account_month_h3` (H3 res 6/8/11)

---

## 1) Strategy (MV layer)

### Goals
- Build monthly map datasets (H3 / settlements / optional subscriber points).
- Support repeated rebuilds for the same month without `DROP PARTITION` by using `ReplacingMergeTree(version)` on marts.
- Keep MV logic simple and deterministic: **ETL prepares normalized facts**, MV only aggregates.

### Data flow
1. **ETL SQL** builds normalized monthly fact table: `stg_account_month`
   - one row per `account_id` per month (`recv_mon` = last day of month)
   - includes coordinates + settlement_id + monthly metrics + flags + `version`
2. **ETL Python** explodes those rows into H3 rows for res `{6,8,11}`: `stg_account_month_h3`
3. **Materialized Views** write into marts:
   - `stg_account_month_h3` → `agg_h3_month`
   - `stg_account_month` → `agg_settlement_month`
   - `stg_account_month` → `points_month` (optional)

### Versioning model
- Each rebuild run uses a single `version` (e.g. `toUnixTimestamp(now())`) applied to all rows inserted for that month.
- Marts use `ReplacingMergeTree(version)`; each key keeps the newest version.
- **Reading marts**: prefer `argMax(metric, version)` grouped by key, avoiding `FINAL`.

### Notes / pitfalls
- MVs only process **new INSERTs** into staging; create marts + MVs **before** first ETL insert.
- For settlement center coordinates:
  - recommended: use an authoritative settlement center table in ETL (not `avg(lat/lng)` over subscribers)
  - if not available, `avg(lat/lng)` is acceptable as a placeholder.
- H3 index type:
  - **recommended**: store `h3_index` as `String` (canonical hex id). It avoids fragile UInt64 conversions.
  - If you insist on `UInt64`, ensure a stable conversion method on both ETL and query side.

---

## 2) DDL (tables + materialized views)

Database name in examples: `point_add`.

### 2.1 Staging tables

```sql
-- 0) Optional
-- CREATE DATABASE IF NOT EXISTS point_add;

-- 1.1 Normalized account-month fact (one row per account_id per month)
CREATE TABLE IF NOT EXISTS point_add.stg_account_month
(
    recv_mon        Date,        -- last day of month
    account_id      UInt64,

    settlement_id   UInt64,
    lat             Float64,
    lng             Float64,

    is_active       UInt8,
    is_sysblock     UInt8,

    charges_cnt     UInt32,
    charges_sum     Decimal(18,2),

    payments_cnt    UInt32,
    payments_sum    Decimal(18,2),

    version         UInt64,       -- rebuild version (e.g., toUnixTimestamp(now()))
    loaded_at       DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY recv_mon
ORDER BY (recv_mon, account_id)
SETTINGS index_granularity = 8192;

-- 1.2 Account-month fact exploded by H3 (recommended: compute h3_index in ETL)
-- RECOMMENDED: use String for h3_index
CREATE TABLE IF NOT EXISTS point_add.stg_account_month_h3
(
    recv_mon        Date,
    account_id      UInt64,

    h3_res          UInt8,
    h3_index        String,

    settlement_id   UInt64,

    is_active       UInt8,
    is_sysblock     UInt8,

    charges_cnt     UInt32,
    charges_sum     Decimal(18,2),

    payments_cnt    UInt32,
    payments_sum    Decimal(18,2),

    version         UInt64,
    loaded_at       DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY recv_mon
ORDER BY (recv_mon, h3_res, h3_index, account_id)
SETTINGS index_granularity = 8192;
```

### 2.2 Mart tables (ReplacingMergeTree)

```sql
-- 2.1 H3 aggregates per month
CREATE TABLE IF NOT EXISTS point_add.agg_h3_month
(
    recv_mon       Date,
    h3_res         UInt8,
    h3_index       String,

    active_cnt     UInt32,
    sysblock_cnt   UInt32,

    charges_cnt    UInt32,
    charges_sum    Decimal(18,2),

    payments_cnt   UInt32,
    payments_sum   Decimal(18,2),

    version        UInt64,
    updated_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY recv_mon
ORDER BY (recv_mon, h3_res, h3_index)
SETTINGS index_granularity = 8192;

-- 2.2 Settlement aggregates per month
CREATE TABLE IF NOT EXISTS point_add.agg_settlement_month
(
    recv_mon        Date,
    settlement_id   UInt64,

    center_lat      Float64,
    center_lng      Float64,

    active_cnt      UInt32,
    sysblock_cnt    UInt32,

    charges_cnt     UInt32,
    charges_sum     Decimal(18,2),

    payments_cnt    UInt32,
    payments_sum    Decimal(18,2),

    version         UInt64,
    updated_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY recv_mon
ORDER BY (recv_mon, settlement_id)
SETTINGS index_granularity = 8192;

-- 2.3 Subscriber points per month (optional, for zoom 15+)
CREATE TABLE IF NOT EXISTS point_add.points_month
(
    recv_mon        Date,
    account_id      UInt64,

    lat             Float64,
    lng             Float64,
    settlement_id   UInt64,

    is_active       UInt8,
    is_sysblock     UInt8,

    charges_sum     Decimal(18,2),
    payments_sum    Decimal(18,2),

    version         UInt64,
    updated_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY recv_mon
ORDER BY (recv_mon, account_id)
SETTINGS index_granularity = 8192;
```

### 2.3 Materialized views

```sql
-- 3.1 MV: H3 staging -> H3 mart
CREATE MATERIALIZED VIEW IF NOT EXISTS point_add.mv_agg_h3_month
TO point_add.agg_h3_month
AS
SELECT
    recv_mon,
    h3_res,
    h3_index,

    sum(is_active)       AS active_cnt,
    sum(is_sysblock)     AS sysblock_cnt,

    sum(charges_cnt)     AS charges_cnt,
    sum(charges_sum)     AS charges_sum,

    sum(payments_cnt)    AS payments_cnt,
    sum(payments_sum)    AS payments_sum,

    version,
    now()                AS updated_at
FROM point_add.stg_account_month_h3
GROUP BY
    recv_mon, h3_res, h3_index, version;

-- 3.2 MV: account-month staging -> settlement mart
-- NOTE: Prefer authoritative settlement centers in ETL; avg(lat/lng) is a fallback.
CREATE MATERIALIZED VIEW IF NOT EXISTS point_add.mv_agg_settlement_month
TO point_add.agg_settlement_month
AS
SELECT
    recv_mon,
    settlement_id,

    avg(lat)             AS center_lat,
    avg(lng)             AS center_lng,

    sum(is_active)       AS active_cnt,
    sum(is_sysblock)     AS sysblock_cnt,

    sum(charges_cnt)     AS charges_cnt,
    sum(charges_sum)     AS charges_sum,

    sum(payments_cnt)    AS payments_cnt,
    sum(payments_sum)    AS payments_sum,

    version,
    now()                AS updated_at
FROM point_add.stg_account_month
GROUP BY
    recv_mon, settlement_id, version;

-- 3.3 MV: account-month staging -> points mart (optional)
CREATE MATERIALIZED VIEW IF NOT EXISTS point_add.mv_points_month
TO point_add.points_month
AS
SELECT
    recv_mon,
    account_id,

    lat,
    lng,
    settlement_id,

    is_active,
    is_sysblock,

    charges_sum,
    payments_sum,

    version,
    now() AS updated_at
FROM point_add.stg_account_month;
```

### 2.4 Recommended read pattern (no FINAL)

```sql
SELECT
  recv_mon, h3_res, h3_index,
  argMax(active_cnt, version)   AS active_cnt,
  argMax(sysblock_cnt, version) AS sysblock_cnt,
  argMax(charges_cnt, version)  AS charges_cnt,
  argMax(charges_sum, version)  AS charges_sum,
  argMax(payments_cnt, version) AS payments_cnt,
  argMax(payments_sum, version) AS payments_sum
FROM point_add.agg_h3_month
WHERE recv_mon = toDate('2026-01-31')
  AND h3_res = 8
GROUP BY recv_mon, h3_res, h3_index;
```

---

## 3) ETL

### 3.1 ETL SQL: populate `stg_account_month`

**Expected sources** (rename columns as needed):
- `point_add.point_customer_active(account_id, recv_mon)`
- `point_add.point_customer_sysblock(account_id, recv_mon)`
- `point_add.acct_addr_coordinates(account_id, settlement_id, lat, lng, updated_at)`
- `point_add.dtall(account_id, dt, amount)`  — charges
- `point_add.pt(account_id, dt, amount, pay_method)` — payments

```sql
-- ========= PARAMETERS =========
-- :month_start  e.g. '2026-01-01'
-- :version      e.g. toUnixTimestamp(now()) captured ONCE per run

WITH
    toDate(:month_start)                               AS month_start,
    toLastDayOfMonth(month_start)                      AS recv_mon,
    toUInt64(:version)                                 AS version,

coords AS
(
    SELECT
        account_id,
        argMax(settlement_id, updated_at) AS settlement_id,
        argMax(lat,          updated_at) AS lat,
        argMax(lng,          updated_at) AS lng
    FROM point_add.acct_addr_coordinates
    WHERE lat != 0 AND lng != 0
    GROUP BY account_id
),

active AS
(
    SELECT account_id, 1 AS is_active
    FROM point_add.point_customer_active
    WHERE recv_mon = recv_mon
),

sysblock AS
(
    SELECT account_id, 1 AS is_sysblock
    FROM point_add.point_customer_sysblock
    WHERE recv_mon = recv_mon
),

charges AS
(
    SELECT
        account_id,
        count()                     AS charges_cnt,
        toDecimal64(sum(amount), 2) AS charges_sum
    FROM point_add.dtall
    WHERE dt >= month_start AND dt < addMonths(month_start, 1)
    GROUP BY account_id
),

payments AS
(
    SELECT
        account_id,
        count()                     AS payments_cnt,
        toDecimal64(sum(amount), 2) AS payments_sum
    FROM point_add.pt
    WHERE dt >= month_start AND dt < addMonths(month_start, 1)
      AND pay_method IN (6553998, 103, 104)
    GROUP BY account_id
),

accounts AS
(
    SELECT account_id FROM active
    UNION DISTINCT
    SELECT account_id FROM sysblock
    UNION DISTINCT
    SELECT account_id FROM charges
    UNION DISTINCT
    SELECT account_id FROM payments
    UNION DISTINCT
    SELECT account_id FROM coords
)

INSERT INTO point_add.stg_account_month
(
    recv_mon, account_id,
    settlement_id, lat, lng,
    is_active, is_sysblock,
    charges_cnt, charges_sum,
    payments_cnt, payments_sum,
    version
)
SELECT
    recv_mon                                 AS recv_mon,
    a.account_id                             AS account_id,

    c.settlement_id                          AS settlement_id,
    c.lat                                    AS lat,
    c.lng                                    AS lng,

    toUInt8(ifNull(act.is_active, 0))        AS is_active,
    toUInt8(ifNull(sb.is_sysblock, 0))       AS is_sysblock,

    toUInt32(ifNull(ch.charges_cnt, 0))      AS charges_cnt,
    ifNull(ch.charges_sum, toDecimal64(0,2)) AS charges_sum,

    toUInt32(ifNull(p.payments_cnt, 0))      AS payments_cnt,
    ifNull(p.payments_sum, toDecimal64(0,2)) AS payments_sum,

    version                                  AS version
FROM accounts a
LEFT JOIN coords   c   USING (account_id)
LEFT JOIN active   act USING (account_id)
LEFT JOIN sysblock sb  USING (account_id)
LEFT JOIN charges  ch  USING (account_id)
LEFT JOIN payments p   USING (account_id)
WHERE
    c.lat IS NOT NULL AND c.lng IS NOT NULL;
```

### 3.2 ETL Python: build `stg_account_month_h3` for res 6/8/11

Install:
```bash
pip install h3 clickhouse-connect
```

Script template:
```python
import h3
import clickhouse_connect

H3_RES_LIST = [6, 8, 11]

def iter_rows(client, recv_mon: str, version: int, fetch_size: int = 100_000):
    base = '''
        SELECT
            recv_mon,
            account_id,
            settlement_id,
            lat,
            lng,
            is_active,
            is_sysblock,
            charges_cnt,
            charges_sum,
            payments_cnt,
            payments_sum,
            version
        FROM point_add.stg_account_month
        WHERE recv_mon = toDate(%(recv_mon)s)
          AND version = %(version)s
          AND lat IS NOT NULL AND lng IS NOT NULL
    '''
    offset = 0
    while True:
        page = client.query(
            base + f" LIMIT {fetch_size} OFFSET {offset}",
            parameters={"recv_mon": recv_mon, "version": version},
        )
        rows = page.result_rows
        if not rows:
            break
        for r in rows:
            yield r
        offset += fetch_size

def explode_h3(rows):
    for (
        recv_mon, account_id, settlement_id, lat, lng,
        is_active, is_sysblock, charges_cnt, charges_sum,
        payments_cnt, payments_sum, version
    ) in rows:
        for res in H3_RES_LIST:
            h3_index = h3.latlng_to_cell(lat, lng, res)  # canonical string
            yield (
                recv_mon, account_id,
                res, h3_index,
                settlement_id,
                is_active, is_sysblock,
                charges_cnt, charges_sum,
                payments_cnt, payments_sum,
                version
            )

def main():
    # --- config ---
    ch_host = "localhost"
    ch_port = 8123
    ch_user = "default"
    ch_password = ""

    recv_mon = "2026-01-31"   # last day of month
    version = 1738291200      # same as in SQL stage

    client = clickhouse_connect.get_client(
        host=ch_host, port=ch_port, username=ch_user, password=ch_password
    )

    insert_table = "point_add.stg_account_month_h3"
    cols = [
        "recv_mon", "account_id",
        "h3_res", "h3_index",
        "settlement_id",
        "is_active", "is_sysblock",
        "charges_cnt", "charges_sum",
        "payments_cnt", "payments_sum",
        "version",
    ]

    batch = []
    batch_size = 50_000

    for row in explode_h3(iter_rows(client, recv_mon, version)):
        batch.append(row)
        if len(batch) >= batch_size:
            client.insert(insert_table, batch, column_names=cols)
            batch.clear()

    if batch:
        client.insert(insert_table, batch, column_names=cols)

    print("Inserted H3 staging rows.")

if __name__ == "__main__":
    main()
```

### 3.3 Runbook (one month rebuild)
1) Capture `version` once (e.g. unix timestamp).  
2) Run **ETL SQL** inserting `stg_account_month` for the month.  
3) Run **Python** to insert `stg_account_month_h3` for the same `(recv_mon, version)`.  
4) MVs populate marts automatically.  
5) API reads marts using `argMax(..., version)`.


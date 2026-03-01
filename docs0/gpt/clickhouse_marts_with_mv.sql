-- ============================================================
-- ClickHouse DDL: Staging + Materialized Views + Marts
-- Project: Map metrics by month (H3 / Settlements / Subscribers)
-- Notes:
--   * Marts use ReplacingMergeTree(version) for idempotent rebuilds.
--   * Reads should prefer argMax(..., version) instead of FINAL.
--   * Staging tables are MergeTree (append-only per rebuild version).
-- ============================================================

-- 0) (Optional) Database
-- CREATE DATABASE IF NOT EXISTS point_add;

-- ============================================================
-- 1) STAGING TABLES
-- ============================================================

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
CREATE TABLE IF NOT EXISTS point_add.stg_account_month_h3
(
    recv_mon        Date,
    account_id      UInt64,

    h3_res          UInt8,
    h3_index        UInt64,

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

-- ============================================================
-- 2) MART TABLES (VITRINES)
-- ============================================================

-- 2.1 H3 aggregates per month
CREATE TABLE IF NOT EXISTS point_add.agg_h3_month
(
    recv_mon       Date,
    h3_res         UInt8,
    h3_index       UInt64,

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

-- ============================================================
-- 3) MATERIALIZED VIEWS
-- ============================================================

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
-- NOTE: If you have authoritative settlement centers, prefer joining in ETL and
--       storing center_lat/center_lng in stg_account_month rather than avg().
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

-- ============================================================
-- 4) (REFERENCE) Recommended read patterns (NOT DDL)
-- ============================================================
-- Example: read H3 mart without FINAL:
-- SELECT
--   recv_mon, h3_res, h3_index,
--   argMax(active_cnt, version)   AS active_cnt,
--   argMax(sysblock_cnt, version) AS sysblock_cnt,
--   argMax(charges_cnt, version)  AS charges_cnt,
--   argMax(charges_sum, version)  AS charges_sum,
--   argMax(payments_cnt, version) AS payments_cnt,
--   argMax(payments_sum, version) AS payments_sum
-- FROM point_add.agg_h3_month
-- WHERE recv_mon = toDate('2026-01-31') AND h3_res = 8
-- GROUP BY recv_mon, h3_res, h3_index;

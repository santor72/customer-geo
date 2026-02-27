В1
Агрегаты H3 по месяцу: agg_h3_month
Агрегаты по посёлкам: agg_settlement_month
Точки абонентов на месяц  points_month

В2
Staging: нормализованный “факт” на месяц по абоненту
Витрина H3: ReplacingMergeTree
staging для “развёрнутых” H3 stg_account_month_h3
stg_account_month → agg_settlement_month

Ниже — шаблоны ETL в двух частях:

SQL: собрать point_add.stg_account_month из point_customer_*, acct_addr_coordinates, dtall, pt

Python: посчитать point_add.stg_account_month_h3 на резолюциях 6/8/11 через h3 (h3-py) и загрузить в ClickHouse

Я использую ваши сущности из описания (account_id, recv_mon = последний день месяца, ad = acct_addr_coordinates, bx24_location, платежные методы и пр.). Если у вас названия колонок чуть отличаются — это “правка 1 раз”.

1) ETL SQL шаблон: stg_account_month
Параметры

:month = '2026-01-01' (любой день месяца) или :recv_mon = '2026-01-31'

:version = toUnixTimestamp(now()) (фиксируйте один version на весь прогон)

Я покажу вариант через month_start → recv_mon.

1.1. Рекомендуемая логика (через CTE)
-- ====== PARAMETERS (задайте через ваш оркестратор) ======
-- month_start = '2026-01-01'
-- version     = 1738291200

WITH
    toDate(:month_start)                               AS month_start,
    toLastDayOfMonth(month_start)                      AS recv_mon,
    toUInt64(:version)                                 AS version,

    -- если у вас платежи в копейках/центах — оставьте Decimal как есть
    -- если в float — приводите к Decimal(18,2)

-- ====== 1) coordinates: account_id -> (settlement_id, lat, lng) ======
coords AS
(
    SELECT
        account_id,
        -- выберите правило "последняя координата": maxBy по updated_at/created_at
        argMax(settlement_id, updated_at) AS settlement_id,
        argMax(lat,          updated_at) AS lat,
        argMax(lng,          updated_at) AS lng
    FROM point_add.acct_addr_coordinates
    WHERE lat != 0 AND lng != 0
    GROUP BY account_id
),

-- ====== 2) flags by month ======
active AS
(
    SELECT
        account_id,
        1 AS is_active
    FROM point_add.point_customer_active
    WHERE recv_mon = recv_mon
),
sysblock AS
(
    SELECT
        account_id,
        1 AS is_sysblock
    FROM point_add.point_customer_sysblock
    WHERE recv_mon = recv_mon
),

-- ====== 3) charges in month (dtall) ======
charges AS
(
    SELECT
        account_id,
        count()                                              AS charges_cnt,
        toDecimal64(sum(amount), 2)                          AS charges_sum
    FROM point_add.dtall
    WHERE
        dt >= month_start
        AND dt < addMonths(month_start, 1)
        -- если надо — фильтр по типам начислений
    GROUP BY account_id
),

-- ====== 4) payments in month (pt) ======
payments AS
(
    SELECT
        account_id,
        count()                                              AS payments_cnt,
        toDecimal64(sum(amount), 2)                          AS payments_sum
    FROM point_add.pt
    WHERE
        dt >= month_start
        AND dt < addMonths(month_start, 1)
        AND pay_method IN (6553998, 103, 104)               -- ваш фильтр
    GROUP BY account_id
),

-- ====== 5) base set of accounts for the month ======
-- Важно: чтобы не потерять абонента без платежей/начислений, берём union источников
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

-- ====== FINAL INSERT ======
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
    recv_mon                               AS recv_mon,
    a.account_id                           AS account_id,

    c.settlement_id                        AS settlement_id,
    c.lat                                  AS lat,
    c.lng                                  AS lng,

    toUInt8(ifNull(act.is_active, 0))      AS is_active,
    toUInt8(ifNull(sb.is_sysblock, 0))     AS is_sysblock,

    toUInt32(ifNull(ch.charges_cnt, 0))    AS charges_cnt,
    toDecimal64(ifNull(ch.charges_sum, toDecimal64(0,2)), 2) AS charges_sum,

    toUInt32(ifNull(p.payments_cnt, 0))    AS payments_cnt,
    toDecimal64(ifNull(p.payments_sum, toDecimal64(0,2)), 2) AS payments_sum,

    version                                AS version
FROM accounts a
LEFT JOIN coords   c   USING (account_id)
LEFT JOIN active   act USING (account_id)
LEFT JOIN sysblock sb  USING (account_id)
LEFT JOIN charges  ch  USING (account_id)
LEFT JOIN payments p   USING (account_id)
WHERE
    -- если хотите исключить тех, у кого нет координат:
    c.lat IS NOT NULL AND c.lng IS NOT NULL;
Что стоит проверить/решить явно

В acct_addr_coordinates я использовал updated_at. Если у вас другое поле времени — замените в argMax(...).

В dtall/pt я использовал dt и amount. Замените под ваши поля (pay_date, sum, value, и т.д.).

Если важно сохранять абонентов без координат — уберите WHERE c.lat IS NOT NULL... и храните NULL, а фронт/агрегации решат, что делать.

2) Python: посчитать stg_account_month_h3 (res 6/8/11) и загрузить в CH
Идея

Берём строки из stg_account_month за recv_mon и version

Для каждой строки считаем h3_index для резолюций [6,8,11]

Пишем в CSV/TSV и грузим в stg_account_month_h3

Ниже код “как шаблон”. Он не требует pandas (можно и с pandas, но потоковая обработка надежнее).

Зависимости: pip install h3 clickhouse-connect

import csv
from datetime import date
from typing import Iterable, Tuple

import h3
import clickhouse_connect

H3_RES_LIST = [6, 8, 11]

def iter_stg_rows(client, recv_mon: str, version: int, fetch_size: int = 100_000):
    """
    Stream rows from stg_account_month for a given month+version.
    """
    query = """
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
    """
    # clickhouse-connect returns a result object; we can page via LIMIT/OFFSET or use query_df.
    # For large sets, easiest: use LIMIT/OFFSET loop (OK for monthly batch).
    offset = 0
    while True:
        page = client.query(
            query + f" LIMIT {fetch_size} OFFSET {offset}",
            parameters={"recv_mon": recv_mon, "version": version}
        )
        rows = page.result_rows
        if not rows:
            break
        for r in rows:
            yield r
        offset += fetch_size

def explode_to_h3(rows: Iterable[tuple]) -> Iterable[Tuple]:
    """
    Convert stg_account_month rows to stg_account_month_h3 rows for H3_RES_LIST.
    """
    for (
        recv_mon, account_id, settlement_id, lat, lng,
        is_active, is_sysblock, charges_cnt, charges_sum,
        payments_cnt, payments_sum, version
    ) in rows:
        # h3-py expects (lat, lng)
        for res in H3_RES_LIST:
            h3_hex = h3.latlng_to_cell(lat, lng, res)   # returns string like '8928308280fffff'
            # Convert to UInt64 if you store UInt64. h3-py returns string; you can keep String in CH to simplify.
            # If you insist on UInt64, you need a conversion method. Safer: store String in CH.
            yield (
                recv_mon, account_id,
                res, h3_hex,
                settlement_id,
                is_active, is_sysblock,
                charges_cnt, charges_sum,
                payments_cnt, payments_sum,
                version
            )

def main():
    # ---- config ----
    ch_host = "localhost"
    ch_port = 8123
    ch_user = "default"
    ch_password = ""
    recv_mon = "2026-01-31"
    version = 1738291200  # фиксируйте один version на весь прогон

    client = clickhouse_connect.get_client(
        host=ch_host, port=ch_port, username=ch_user, password=ch_password
    )

    # If your stg_account_month_h3.h3_index is UInt64, I'd strongly recommend changing it to String.
    # Otherwise conversion becomes implementation-specific.
    # Here we assume h3_index is String in the target table OR you adjust conversion.

    rows = iter_stg_rows(client, recv_mon=recv_mon, version=version)

    # Insert in batches
    insert_sql = """
        INSERT INTO point_add.stg_account_month_h3
        (recv_mon, account_id, h3_res, h3_index, settlement_id,
         is_active, is_sysblock, charges_cnt, charges_sum, payments_cnt, payments_sum, version)
        VALUES
    """

    batch = []
    batch_size = 50_000

    for out_row in explode_to_h3(rows):
        batch.append(out_row)
        if len(batch) >= batch_size:
            client.insert(
                "point_add.stg_account_month_h3",
                batch,
                column_names=[
                    "recv_mon", "account_id", "h3_res", "h3_index", "settlement_id",
                    "is_active", "is_sysblock", "charges_cnt", "charges_sum",
                    "payments_cnt", "payments_sum", "version"
                ],
            )
            batch.clear()

    if batch:
        client.insert(
            "point_add.stg_account_month_h3",
            batch,
            column_names=[
                "recv_mon", "account_id", "h3_res", "h3_index", "settlement_id",
                "is_active", "is_sysblock", "charges_cnt", "charges_sum",
                "payments_cnt", "payments_sum", "version"
            ],
        )

    print("Done.")

if __name__ == "__main__":
    main()
Важный момент про тип h3_index

В моём DDL ранее был UInt64. На практике проще и безопаснее хранить H3 как String (8928308280fffff) — тогда:

нет зависимостей от конкретного способа “упаковки” H3 в 64-бит

проще отлаживать и сверять

быстрее интеграция с фронтом

Если вы готовы — скажите, и я дам миграцию:

ALTER TABLE ... MODIFY COLUMN h3_index String;

и аналогично в agg_h3_month.

Минимальный “runbook” на 1 месяц

(ETL SQL) INSERT stg_account_month с фиксированным version

(Python) посчитать stg_account_month_h3 для (recv_mon, version) и INSERT

MV сами заполнят:

agg_h3_month

agg_settlement_month

points_month (если включили)

Если хочешь, я следующим сообщением сделаю конкретный .sql файл (как мы делали DDL) с:

шаблоном -- set month_start=...; set version=...;

блоком INSERT в stg_account_month

проверками качества (сколько без координат, сколько попало в staging, топ settlement по
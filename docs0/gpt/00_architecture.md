# GIS Financial Map (Stage 0 → Prod)

Этот документ фиксирует целевую архитектуру, контракты API и схему витрин (агрегатов) для приложения, которое визуализирует финансовые и операционные метрики (активность/блокировки/списания/платежи) на карте по месяцам.

## 1. Цель и ключевые сценарии

### 1.1. Что пользователь делает
1. Выбирает **месяц** (формат `YYYY-MM`).
2. Открывает карту и видит агрегаты по территории.
3. При приближении (zoom) слой «детализируется»:
   - 1–10: гексы (H3) + поселки
   - 11–14: поселки (и при наличии — полигоны поселков)
   - 15+: абоненты (точки) + поселки
4. Меняет показатель (например, `payments_sum`, `charges_sum`, `active_cnt`, доля блокировок), применяет фильтры.

### 1.2. Бизнес‑правило периода (важно)
- Внешний API принимает `month=YYYY-MM`.
- Внутри сервисов этот месяц приводится к `recv_mon = last_day(month)`.
- Все витрины и расчеты привязаны к `recv_mon`.

### 1.3. Семантика метрик (фиксируем)
Для карты месяца `M`:
- **active/sysblock**: состояние/признак для `recv_mon` месяца `M`.
- **payments/charges**: транзакции *внутри календарного месяца* `M` (например, по `date_trunc('month', dt) == month(M)`).

> Если у вас нужна другая семантика (например «состояние на конец месяца для сумм»), меняется только слой витрин: принцип архитектуры остается.

## 2. Высокоуровневая архитектура

### 2.1. Компоненты
1. **ClickHouse**
   - хранит исходные таблицы
   - хранит витрины (агрегаты по H3 / по поселкам / (опционально) по точкам)

2. **ETL/Orchestrator** (cron / Airflow / Dagster / n8n)
   - ежемесячный/ежедневный пересчет витрин
   - контроль версий данных

3. **Backend API** (рекомендуется FastAPI; возможно Flask)
   - выдает слои карты
   - авторизация, rate-limit
   - кэширование ответов (Redis)

4. **Redis**
   - кэш bbox‑запросов по ключу, учитывающему `data_version`

5. **Frontend** (React/Vue + kepler.gl / deck.gl)
   - выбор месяца и метрики
   - подгрузка данных по bbox
   - переключение слоев по zoom

### 2.2. Поток данных
1. Источники → ClickHouse (как есть)
2. ETL строит витрины:
   - `agg_h3_month`
   - `agg_settlement_month`
   - `points_month` (опционально)
3. Frontend делает bbox‑запросы к API
4. API читает витрины, отдает GeoJSON/JSON (или MVT на следующем этапе)
5. Redis кеширует ответы

## 3. Схема данных и витрины

### 3.1. Сырые источники (логическая модель)
- `point_customer_active` — признак активного абонента (по `recv_mon`, `account_id`).
- `point_customer_sysblock` — признак системной блокировки (по `recv_mon`, `account_id`).
- `acct_addr_coordinates` — координаты/адрес абонента (по `account_id`).
- `pt` / `dtall` — платежи/списания с датами и суммами.
- `bx24_location` — справочник поселков (id, coords, name, etc.).

> Точные названия/поля уточняются по вашей схеме CH, но витрины ниже остаются неизменными по смыслу.

### 3.2. Гео‑нормализация (единые правила)
- Координаты считаются валидными, если:
  - `-90 ≤ lat ≤ 90`, `-180 ≤ lon ≤ 180`, и не (0,0)
- Если координат нет:
  - абонент не попадает в H3‑витрину и точечный слой
  - может попадать в витрину по поселкам только если есть `settlement_id` и координаты поселка

### 3.3. Витрина: агрегаты H3 по месяцу
**Назначение:** для zoom 1–10 и частично 11–14.

**Таблица:** `point_add.agg_h3_month`

**Поля (минимум):**
- `recv_mon Date` — последний день месяца
- `h3_res UInt8`
- `h3_index UInt64` *(или String — если так удобнее)*
- `active_cnt UInt32`
- `sysblock_cnt UInt32`
- `charges_cnt UInt32`
- `charges_sum Float64`
- `payments_cnt UInt32`
- `payments_sum Float64`

**Производные (опционально в ответе API):**
- `block_rate = sysblock_cnt / nullIf(active_cnt,0)`
- `net = payments_sum - charges_sum`
- `arpu = payments_sum / nullIf(active_cnt,0)`

### 3.4. Витрина: агрегаты по поселкам
**Назначение:** zoom 1–14.

**Таблица:** `point_add.agg_settlement_month`

**Поля (минимум):**
- `recv_mon Date`
- `settlement_id UInt64` (bx24_location.id)
- `settlement_lat Float64`
- `settlement_lon Float64`
- метрики как в H3‑витрине

### 3.5. Витрина: точки абонентов по месяцу (опционально)
**Назначение:** zoom 15+.

**Таблица:** `point_add.points_month`

**Поля (минимум):**
- `recv_mon Date`
- `account_id UInt64`
- `lat Float64`
- `lon Float64`
- `settlement_id Nullable(UInt64)`
- `is_active UInt8`
- `is_sysblock UInt8`

Опционально добавить агрегаты месяца на точку:
- `charges_sum_m Float64`
- `payments_sum_m Float64`

## 4. Контракты API

Все ответы должны содержать `data_version` (или заголовок) для корректного клиентского/серверного кэша.

### 4.1. Список доступных месяцев
`GET /api/v1/months`

**Response**
```json
{
  "months": ["2025-11", "2025-12", "2026-01"],
  "latest": "2026-01"
}
```

### 4.2. Версия данных месяца
`GET /api/v1/version?month=YYYY-MM`

**Response**
```json
{
  "month": "2026-01",
  "recv_mon": "2026-01-31",
  "data_version": "2026-01@build_2026-02-03T01:10:00Z"
}
```

### 4.3. H3 слой (гексы)
`GET /api/v1/layers/h3?month=YYYY-MM&bbox=minLon,minLat,maxLon,maxLat&metric=payments_sum&h3_res=7`

**Response (GeoJSON):**
- FeatureCollection с Polygon геометрией гекса и properties метрик

**Примечания:**
- `metric` задает, что использовать для цвета/heat.
- `h3_res` можно задавать автоматически на сервере по `zoom`.

### 4.4. Поселки
`GET /api/v1/layers/settlements?month=YYYY-MM&bbox=minLon,minLat,maxLon,maxLat`

**Response (GeoJSON):**
- FeatureCollection с Point геометрией поселка и properties метрик.

### 4.5. Абоненты (точки)
`GET /api/v1/layers/subscribers?month=YYYY-MM&bbox=minLon,minLat,maxLon,maxLat&limit=5000&cursor=...`

**Response:**
```json
{
  "data_version": "...",
  "items": [
    {
      "account_id": 123,
      "lat": 55.75,
      "lon": 37.61,
      "is_active": 1,
      "is_sysblock": 0
    }
  ],
  "next_cursor": "..."
}
```

### 4.6. Общие параметры и ошибки
- `bbox` обязателен для всех картографических ручек
- сервер должен ограничивать:
  - максимальную площадь bbox на слоях точек
  - `limit` для subscribers
- стандартные коды:
  - `400` invalid params
  - `401/403` auth
  - `429` rate-limit
  - `500` internal

## 5. Масштабирование и производительность

### 5.1. Предагрегация — ключ
- H3/поселки должны читаться из витрин.
- Лайв‑джойны сырых таблиц допустимы только в прототипе.

### 5.2. Кэш
- Redis ключ: `layer:{layer}:month:{month}:bbox:{bbox}:metric:{metric}:zoom:{zoom}:v:{data_version}`
- TTL: 1–24h
- Инвалидация: смена `data_version`

### 5.3. Векторные тайлы (следующий уровень)
Когда GeoJSON начнет быть тяжёлым:
- добавить `GET /mvt/...` и отдавать MVT для гексов и поселков

## 6. Безопасность и эксплуатация (минимум)
- Auth (JWT/Keycloak) для API
- Rate limiting на публичных ручках
- Логи запросов + метрики p95/p99
- Отдельные роли CH: readonly для API, write для ETL

---

## Приложение A. Маппинг zoom → слой
- `zoom 1..10`: `h3(res 6–7) + settlements`
- `zoom 11..14`: `settlements (+ optional h3 res 8–9)`
- `zoom 15+`: `subscribers + settlements`

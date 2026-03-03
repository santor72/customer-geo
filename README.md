# Карта клиентов с данными из учетной системы


### Что сделано

Каркас backend и минимальный frontend для Phase 1 собраны, метрики active_cnt, charges_sum_m, payments_sum_m встроены.

Backend FastAPI с ручками months, subscribers, settlements, h3 и валидацией bbox/limit/h3_res.
Подключение к MySQL через SQLAlchemy + pymysql, выборка из витрин agg_customer_cur, agg_settlement_cur, agg_h3_cur.
Минимальный frontend на MapLibre + deck.gl + Supercluster с переключением слоев по zoom.

схема опциональна, и таблицы берутся без префикса, если MYSQL_SCHEMA пустой
слой абонентов включается на всех zoom. Кластеризация остается, чтобы не перегружать карту.
Размер клачтера настраиваемый и увеличил в 2 раза для zoom > 10.
CLUSTER_RADIUS_LOW_ZOOM = 80
CLUSTER_RADIUS_HIGH_ZOOM = 120
агрегация метрик в кластеризацию и текстовые подписи на кластерах.
что показывает подпись на кластеры:
active_cnt/payments_sum_m
tooltip на кластеры подпись
A: active_cnt
S: sysblock_cnt
P: payments_sum_m
C: charges_sum_m

*переключатели слоев*
UI
H3
Поселки
Клиенты
Подписи


Файлы
main.py
db.py
config.py
requirements.txt
index.html
app.js
env.sample

Кастомный HTML‑tooltip (карточка).
Разные типы и стили для H3, Поселки, Кластеры, Абоненты.
Форматирование чисел/денег по ru-RU.


кнопка справа по центру и поиск поселков как geosearch.
UI‑контрол справа посередине с кнопкой Поиск, раскрывающей панель.
Поиск по названию поселка с подсказками.
По клику — flyTo к поселку.
Панель закрывается по Esc и по клику вне.

Геокодинг
интеграция DaData suggest + Yandex geocode и добавил переключатель режимов в правой панели поиска.
Что добавлено
В режиме Адрес: подсказки через DaData, по клику — геокодирование через Яндекс и плавный flyTo.
Backend‑прокси для DaData и Яндекс.

## Как запустить

### Backend
cd /Users/santor/Documents/work/customer-geo/backend
pip install -r requirements.txt
Создай .env в корне проекта по образцу env.sample.
* Запуск API *
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
### Frontend
cd /Users/santor/Documents/work/customer-geo/frontend
python -m http.server 5173
Открыть в браузере http://localhost:5173.
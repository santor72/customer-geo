-- Создание базы данных (в ClickHouse вместо SCHEMA используется DATABASE)
CREATE DATABASE IF NOT EXISTS schema_customer;

-- Таблица 1: agg_customer_cur
CREATE TABLE IF NOT EXISTS schema_customer.agg_customer_cur
(
    recv_mon             String,
    account_id           Int32,
    settlement_id        Int32,
    lat                  Float64,
    lng                  Float64,
    is_active            Bool,
    is_sysblock          Bool,
    charges_sum_m        Float64,
    payments_sum_m       Float64,
    version_ts           DateTime DEFAULT now()
)
ENGINE = ReplaceMergingTree(version_ts)
PARTITION BY recv_mon
ORDER BY (account_id, recv_mon)
PRIMARY KEY (account_id, recv_mon)
COMMENT 'Витрина данных о клиентах текущего месяца
месяц recv_mon YYYY-MM-DD - последний день месяца к которому относятся данные';


-- Таблица 2: agg_h3_cur
CREATE TABLE IF NOT EXISTS schema_customer.agg_h3_cur
(
    recv_mon             String,
    h3_res               Int32,
    h3_index             Int64,
    active_cnt           Int32,
    sysblock_cnt         Bool,
    charges_sum_m        Float64,
    payments_sum_m       Float64,
    charges_cnt_m        Int32,
    payments_cnt_m       Int32,
    version_ts           DateTime DEFAULT now()
)
ENGINE = ReplaceMergingTree(version_ts)
PARTITION BY recv_mon
ORDER BY (recv_mon, h3_res, h3_index) 
PRIMARY KEY (recv_mon, h3_res, h3_index)
COMMENT 'Витрина данных о гексагонах текущего месяца
месяц recv_mon YYYY-MM-DD - последний день месяца к которому относятся данные';


CREATE TABLE IF NOT EXISTS schema_customer.agg_settlement_cur
(
    recv_mon             String NOT NULL,
    settlement_id        Int32,
    lat                  Float64,
    lng                  Float64,
    active_cnt           Int32,
    sysblock_cnt         Bool,
    charges_sum_m        Float64,
    payments_sum_m       Float64,
    charges_cnt_m        Int32,
    payments_cnt_m       Int32,
    version_ts           DateTime DEFAULT now()
)
ENGINE = ReplaceMergingTree(version_ts)
PARTITION BY recv_mon
ORDER BY (settlement_id, recv_mon) -- Соответствует первичному ключу MySQL (settlement_id, recv_mon)
PRIMARY KEY (settlement_id, recv_mon)
COMMENT 'Витрина данных о поселках  текущего месяца
месяц recv_mon YYYY-MM-DD - последний день месяца к которому относятся данные';
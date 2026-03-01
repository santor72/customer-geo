CREATE SCHEMA schema_customer;

CREATE  TABLE schema_customer.agg_customer_cur ( 
	recv_mon             VARCHAR(10)       ,
	account_id           INT       ,
	settlement_id        INT       ,
	lat                  NUMERIC       ,
	lng                  NUMERIC       ,
	is_active            BOOLEAN       ,
	is_sysblock          BOOLEAN       ,
	charges_sum_m        DOUBLE       ,
	payments_sum_m       DOUBLE       
 );

CREATE INDEX idx_agg_customer_month ON schema_customer.agg_customer_cur ( recv_mon, account_id );

ALTER TABLE schema_customer.agg_customer_cur COMMENT 'Витрина данных о клиентах текущего месяца
месяц recv_mon YYYY-MM-DD - последний день месяца к которому относятся данные';

ALTER TABLE schema_customer.agg_customer_cur MODIFY recv_mon VARCHAR(10)     COMMENT 'Месяц отчета';

ALTER TABLE schema_customer.agg_customer_cur MODIFY account_id INT     COMMENT 'идентификатор клиента';

ALTER TABLE schema_customer.agg_customer_cur MODIFY settlement_id INT     COMMENT 'идентификатор поселка';

ALTER TABLE schema_customer.agg_customer_cur MODIFY lat NUMERIC     COMMENT 'Широта';

ALTER TABLE schema_customer.agg_customer_cur MODIFY lng NUMERIC     COMMENT 'Долгота';

ALTER TABLE schema_customer.agg_customer_cur MODIFY is_active BOOLEAN     COMMENT 'Признак активного клиента';

ALTER TABLE schema_customer.agg_customer_cur MODIFY is_sysblock BOOLEAN     COMMENT 'Признак заблокированного клиента';

ALTER TABLE schema_customer.agg_customer_cur MODIFY charges_sum_m DOUBLE     COMMENT 'Сумма списаний за период';

ALTER TABLE schema_customer.agg_customer_cur MODIFY payments_sum_m DOUBLE     COMMENT 'Сумма платежей клиента за период';

CREATE  TABLE schema_customer.agg_h3_cur ( 
	recv_mon             VARCHAR(10)       ,
	h3_res               INT       ,
	h3_index             BIGINT       ,
	active_cnt           INT       ,
	sysblock_cnt         BOOLEAN       ,
	charges_sum_m        DOUBLE       ,
	payments_sum_m       DOUBLE       ,
	charges_cnt_m        INT       ,
	payments_cnt_m       INT       
 );

CREATE INDEX idx_agg_h3_month ON schema_customer.agg_h3_cur ( recv_mon, h3_res, h3_index );

ALTER TABLE schema_customer.agg_h3_cur COMMENT 'Витрина данных о гексагонах текущего месяца
месяц recv_mon YYYY-MM-DD - последний день месяца к которому относятся данные';

ALTER TABLE schema_customer.agg_h3_cur MODIFY recv_mon VARCHAR(10)     COMMENT 'Месяц отчета';

ALTER TABLE schema_customer.agg_h3_cur MODIFY h3_res INT     COMMENT 'h3 resolution';

ALTER TABLE schema_customer.agg_h3_cur MODIFY h3_index BIGINT     COMMENT 'H3 index';

ALTER TABLE schema_customer.agg_h3_cur MODIFY active_cnt INT     COMMENT 'Количство  активных клиентов';

ALTER TABLE schema_customer.agg_h3_cur MODIFY sysblock_cnt BOOLEAN     COMMENT 'Количство  заблокированных клиентов';

ALTER TABLE schema_customer.agg_h3_cur MODIFY charges_sum_m DOUBLE     COMMENT 'Сумма списаний за период';

ALTER TABLE schema_customer.agg_h3_cur MODIFY payments_sum_m DOUBLE     COMMENT 'Сумма платежей клиента за период';

ALTER TABLE schema_customer.agg_h3_cur MODIFY charges_cnt_m INT     COMMENT 'Количество  списаний за период';

ALTER TABLE schema_customer.agg_h3_cur MODIFY payments_cnt_m INT     COMMENT 'Количество  платежей за период';

CREATE  TABLE schema_customer.agg_settlement_cur ( 
	recv_mon             VARCHAR(10)    NOT NULL   PRIMARY KEY,
	settlement_id        INT       ,
	lat                  NUMERIC       ,
	lng                  NUMERIC       ,
	active_cnt           INT       ,
	sysblock_cnt         BOOLEAN       ,
	charges_sum_m        DOUBLE       ,
	payments_sum_m       DOUBLE       ,
	charges_cnt_m        INT       ,
	payments_cnt_m       INT       
 );

CREATE INDEX idx_agg_settlement_month ON schema_customer.agg_settlement_cur ( recv_mon, settlement_id );

ALTER TABLE schema_customer.agg_settlement_cur COMMENT 'Витрина данных о поселках  текущего месяца
месяц recv_mon YYYY-MM-DD - последний день месяца к которому относятся данные';

ALTER TABLE schema_customer.agg_settlement_cur MODIFY recv_mon VARCHAR(10)  NOT NULL   COMMENT 'Месяц отчета';

ALTER TABLE schema_customer.agg_settlement_cur MODIFY settlement_id INT     COMMENT 'идентификатор поселка';

ALTER TABLE schema_customer.agg_settlement_cur MODIFY lat NUMERIC     COMMENT 'Широта';

ALTER TABLE schema_customer.agg_settlement_cur MODIFY lng NUMERIC     COMMENT 'Долгота';

ALTER TABLE schema_customer.agg_settlement_cur MODIFY active_cnt INT     COMMENT 'Количство  активных клиентов';

ALTER TABLE schema_customer.agg_settlement_cur MODIFY sysblock_cnt BOOLEAN     COMMENT 'Количство  заблокированных клиентов';

ALTER TABLE schema_customer.agg_settlement_cur MODIFY charges_sum_m DOUBLE     COMMENT 'Сумма списаний за период';

ALTER TABLE schema_customer.agg_settlement_cur MODIFY payments_sum_m DOUBLE     COMMENT 'Сумма платежей клиента за период';

ALTER TABLE schema_customer.agg_settlement_cur MODIFY charges_cnt_m INT     COMMENT 'Количество  списаний за период';

ALTER TABLE schema_customer.agg_settlement_cur MODIFY payments_cnt_m INT     COMMENT 'Количество  платежей за период';


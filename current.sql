drop TABLE agg_customer_cur;
CREATE  TABLE agg_customer_cur ( 
	recv_mon             VARCHAR(10)       ,
	account_id           INT       ,
	settlement_id        INT       ,
	lat                  double       ,
	lng                  double       ,
	is_active            BOOLEAN       ,
	is_sysblock          BOOLEAN       ,
	charges_sum_m        DOUBLE       ,
	payments_sum_m       DOUBLE       ,
	charges_sum_prev        DOUBLE       ,
	payments_sum_prev       DOUBLE       
 );

CREATE INDEX idx_agg_customer_month ON agg_customer_cur ( recv_mon, account_id );

ALTER TABLE agg_customer_cur COMMENT 'Витрина данных о клиентах текущего месяца
месяц recv_mon YYYY-MM-DD - последний день месяца к которому относятся данные';

ALTER TABLE agg_customer_cur MODIFY recv_mon VARCHAR(10)     COMMENT 'Месяц отчета';

ALTER TABLE agg_customer_cur MODIFY account_id INT     COMMENT 'идентификатор клиента';

ALTER TABLE agg_customer_cur MODIFY settlement_id INT     COMMENT 'идентификатор поселка';

ALTER TABLE agg_customer_cur MODIFY lat double     COMMENT 'Широта';

ALTER TABLE agg_customer_cur MODIFY lng double     COMMENT 'Долгота';

ALTER TABLE agg_customer_cur MODIFY is_active BOOLEAN     COMMENT 'Признак активного клиента';

ALTER TABLE agg_customer_cur MODIFY is_sysblock BOOLEAN     COMMENT 'Признак заблокированного клиента';

ALTER TABLE agg_customer_cur MODIFY charges_sum_m DOUBLE     COMMENT 'Сумма списаний за период';

ALTER TABLE agg_customer_cur MODIFY payments_sum_m DOUBLE     COMMENT 'Сумма платежей клиента за период';

ALTER TABLE agg_customer_cur MODIFY charges_sum_prev DOUBLE     COMMENT 'Сумма списаний за прошлый месяц';

ALTER TABLE agg_customer_cur MODIFY payments_sum_prev DOUBLE     COMMENT 'Сумма платежей клиента за прошлый месяц';

CREATE  TABLE agg_h3_cur ( 
	recv_mon             VARCHAR(10)       ,
	h3_res               INT       ,
	h3_index             BIGINT       ,
	active_cnt           INT       ,
	sysblock_cnt         INT       ,
	charges_sum_m        DOUBLE       ,
	payments_sum_m       DOUBLE       ,
	charges_cnt_m        INT       ,
	payments_cnt_m       INT    ,
	charges_sum_p        DOUBLE       ,
	payments_sum_p       DOUBLE          
 );

CREATE INDEX idx_agg_h3_month ON agg_h3_cur ( recv_mon, h3_res, h3_index );

ALTER TABLE agg_h3_cur COMMENT 'Витрина данных о гексагонах текущего месяца
месяц recv_mon YYYY-MM-DD - последний день месяца к которому относятся данные';

ALTER TABLE agg_h3_cur MODIFY recv_mon VARCHAR(10)     COMMENT 'Месяц отчета';

ALTER TABLE agg_h3_cur MODIFY h3_res INT     COMMENT 'h3 resolution';

ALTER TABLE agg_h3_cur MODIFY h3_index BIGINT     COMMENT 'H3 index';

ALTER TABLE agg_h3_cur MODIFY active_cnt INT     COMMENT 'Количство  активных клиентов';

ALTER TABLE agg_h3_cur MODIFY sysblock_cnt INT     COMMENT 'Количство  заблокированных клиентов';

ALTER TABLE agg_h3_cur MODIFY charges_sum_m DOUBLE     COMMENT 'Сумма списаний за период';

ALTER TABLE agg_h3_cur MODIFY payments_sum_m DOUBLE     COMMENT 'Сумма платежей клиента за период';

ALTER TABLE agg_h3_cur MODIFY charges_cnt_m INT     COMMENT 'Количество  списаний за период';

ALTER TABLE agg_h3_cur MODIFY payments_cnt_m INT     COMMENT 'Количество  платежей за период';

ALTER TABLE `UTM5point_addons`.agg_h3_cur MODIFY charges_sum_p DOUBLE     COMMENT 'Сумма списаний за прошлый месяц';

ALTER TABLE `UTM5point_addons`.agg_h3_cur MODIFY payments_sum_p DOUBLE     COMMENT 'Сумма платежей клиента за прошлый месяц';

drop TABLE agg_settlement_cur
CREATE  TABLE `UTM5point_addons`.agg_settlement_cur ( 
	recv_mon             VARCHAR(10)    NOT NULL   ,
	settlement_id        INT    NOT NULL   ,
	lat                  DOUBLE       ,
	lng                  DOUBLE       ,
	active_cnt           INT       ,
	sysblock_cnt         INT       ,
	charges_sum_m        DOUBLE       ,
	payments_sum_m       DOUBLE       ,
	charges_cnt_m        INT       ,
	payments_cnt_m       INT       ,
	title                VARCHAR(1000)       ,
	charges_sum_prev     DOUBLE       ,
	payments_sum_prev    DOUBLE       ,
	CONSTRAINT pk_agg_settlement_cur PRIMARY KEY ( recv_mon, settlement_id )
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE INDEX idx_agg_settlement_month ON `UTM5point_addons`.agg_settlement_cur ( recv_mon, settlement_id );

ALTER TABLE `UTM5point_addons`.agg_settlement_cur COMMENT 'Витрина данных о поселках  текущего месяца
месяц recv_mon YYYY-MM-DD - последний день месяца к которому относятся данные';

ALTER TABLE `UTM5point_addons`.agg_settlement_cur MODIFY recv_mon VARCHAR(10)  NOT NULL   COMMENT 'Месяц отчета';

ALTER TABLE `UTM5point_addons`.agg_settlement_cur MODIFY settlement_id INT  NOT NULL   COMMENT 'идентификатор поселка';

ALTER TABLE `UTM5point_addons`.agg_settlement_cur MODIFY lat DOUBLE     COMMENT 'Широта';

ALTER TABLE `UTM5point_addons`.agg_settlement_cur MODIFY lng DOUBLE     COMMENT 'Долгота';

ALTER TABLE `UTM5point_addons`.agg_settlement_cur MODIFY active_cnt INT     COMMENT 'Количство  активных клиентов';

ALTER TABLE `UTM5point_addons`.agg_settlement_cur MODIFY sysblock_cnt INT     COMMENT 'Количство  заблокированных клиентов';

ALTER TABLE `UTM5point_addons`.agg_settlement_cur MODIFY charges_sum_m DOUBLE     COMMENT 'Сумма списаний за период';

ALTER TABLE `UTM5point_addons`.agg_settlement_cur MODIFY payments_sum_m DOUBLE     COMMENT 'Сумма платежей клиента за период';

ALTER TABLE `UTM5point_addons`.agg_settlement_cur MODIFY charges_cnt_m INT     COMMENT 'Количество  списаний за период';

ALTER TABLE `UTM5point_addons`.agg_settlement_cur MODIFY payments_cnt_m INT     COMMENT 'Количество  платежей за период';

ALTER TABLE `UTM5point_addons`.agg_settlement_cur MODIFY charges_sum_prev DOUBLE     COMMENT 'Сумма списаний за прошлый месяц';

ALTER TABLE `UTM5point_addons`.agg_settlement_cur MODIFY payments_sum_prev DOUBLE     COMMENT 'Сумма платежей клиента за прошлый месяц';


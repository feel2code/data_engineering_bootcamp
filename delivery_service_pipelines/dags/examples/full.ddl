--CDM
create table cdm.dm_settlement_report (
	id serial not null,
	restaurant_id varchar(255) not null,
	restaurant_name varchar(255) not null,
	settlement_date date not null,
	orders_count integer not null,
	orders_total_sum numeric(14, 2) not null,
	orders_bonus_payment_sum numeric(14, 2) not null,
	orders_bonus_granted_sum numeric(14, 2) not null,
	order_processing_fee numeric(14, 2) not null,
	restaurant_reward_sum numeric(14, 2) not null
);

ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT dm_settlement_report_id_pkey primary key (id);

ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_settlement_date_check 
CHECK (settlement_date >= '2022-01-01' AND settlement_date < '2500-01-01');

ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_count_check
check (orders_count
>= 0); 
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_total_sum_check
check (orders_total_sum
>= 0);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check
check (orders_bonus_payment_sum
>= 0);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check
check (orders_bonus_granted_sum
>= 0);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_order_processing_fee_check
check (order_processing_fee
>= 0);
ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_restaurant_reward_sum_check
check (restaurant_reward_sum
>= 0);

ALTER TABLE cdm.dm_settlement_report ADD CONSTRAINT dm_settlement_report_unique
UNIQUE (restaurant_id, settlement_date);


--STG
drop table if exists stg.bonussystem_users;
CREATE TABLE stg.bonussystem_users (
	id integer not null,
	order_user_id text not null
);

drop table if exists stg.bonussystem_ranks;
CREATE TABLE stg.bonussystem_ranks (
	id integer not null,
	"name" varchar(2048) not null,
	bonus_percent numeric(19, 5) not null,
	min_payment_threshold numeric(19, 5) not null
);

drop index if exists idx_bonussystem_events__event_ts;
drop table if exists stg.bonussystem_events;
CREATE TABLE stg.bonussystem_events (
	id integer not null,
	event_ts timestamp not null,
	event_type varchar not null,
	event_value text not null
);
CREATE INDEX idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);

drop table if exists stg.ordersystem_orders;
create table stg.ordersystem_orders (
	id serial primary key,
	object_id varchar not null,
	object_value text not null,
	update_ts timestamp not null
);

drop table if exists stg.ordersystem_restaurants;
create table stg.ordersystem_restaurants (
	id serial primary key,
	object_id varchar not null,
	object_value text not null,
	update_ts timestamp not null
);

drop table if exists stg.ordersystem_users;
create table stg.ordersystem_users (
	id serial primary key,
	object_id varchar not null,
	object_value text not null,
	update_ts timestamp not null
);

ALTER TABLE stg.ordersystem_orders ADD CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id);
ALTER TABLE stg.ordersystem_restaurants ADD CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id);
ALTER TABLE stg.ordersystem_users ADD CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id);

--dds
drop table if exists dds.fct_product_sales;
drop table if exists dds.dm_orders;
drop table if exists dds.dm_timestamps;
drop table if exists dds.dm_products;
drop table if exists dds.dm_restaurants;
drop table if exists dds.dm_users;
drop table if exists dds.srv_wf_settings;

CREATE TABLE dds.srv_wf_settings (
	id serial NOT NULL,
	workflow_key varchar not NULL,
	workflow_settings json NOT null,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);

create table dds.dm_users (
	id serial primary key,
	user_id varchar not null,
	user_name varchar not null,
	user_login varchar not null
);

create table dds.dm_restaurants (
	id serial primary key,
	restaurant_id varchar not null,
	restaurant_name varchar not null,
	active_from timestamp not null,
	active_to timestamp not null,
	CONSTRAINT dm_restaurants_restaurant_id_key UNIQUE (restaurant_id)
);

create table dds.dm_products (
	id serial primary key,
	restaurant_id integer not null constraint dm_products_restaurant_id_fkey references dds.dm_restaurants (id),
	product_id varchar not null,
	product_name varchar not null,
	product_price numeric(14, 2) default 0 not null constraint dm_products_product_price_check check (product_price >=0),
	active_from timestamp not null,
	active_to timestamp not null,
	CONSTRAINT dm_products_product_id_key UNIQUE (product_id)
);

create table dds.dm_timestamps (
	id serial primary key,
	ts timestamp not null,
	"year" smallint check ("year" >=2022 and "year" < 2500),
	"month" smallint check ("month" >=1 and "month" <= 12),
	"day" smallint check ("day" >=1 and "day" <= 31),
	"time" time not null,
	"date" date not null
);

create table dds.dm_orders (
	id serial primary key,
	user_id integer not null,
	restaurant_id integer not null,
	timestamp_id integer not null,
	order_key varchar not null,
	order_status varchar not null
);

alter table dds.dm_orders add constraint
dm_orders_user_id_fk foreign key (user_id) references dds.dm_users (id);
alter table dds.dm_orders add constraint
dm_orders_restaurant_id_fk foreign key (restaurant_id) references dds.dm_restaurants (id);
alter table dds.dm_orders add constraint
dm_orders_timestamp_id_fk foreign key (timestamp_id) references dds.dm_timestamps (id);

create table dds.fct_product_sales (
	id serial primary key,
	product_id integer not null,
	order_id integer not null,
	"count" integer default 0 not null constraint fct_product_sales_count_check check ("count" >=0),
	price numeric(14, 2) default 0 not null constraint fct_product_sales_price_check check (price >=0),
	total_sum numeric(14, 2) default 0 not null constraint fct_product_sales_total_sum_check check (total_sum >=0),
	bonus_payment numeric(14, 2) default 0 not null constraint fct_product_sales_bonus_payment_check check (bonus_payment >=0),
	bonus_grant numeric(14, 2) default 0 not null constraint fct_product_sales_bonus_grant_check check (bonus_grant >=0)
);
alter table dds.fct_product_sales add constraint
fct_product_sales_product_id_fk foreign key (product_id) references dds.dm_products (id);
alter table dds.fct_product_sales add constraint
fct_product_sales_order_id_fk foreign key (order_id) references dds.dm_orders (id);


--cdm
CREATE TABLE cdm.srv_wf_settings (
	id serial NOT NULL,
	workflow_key varchar not NULL,
	workflow_settings json NOT null,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);

INSERT INTO cdm.dm_settlement_report
(restaurant_id, restaurant_name, settlement_date, orders_count,
 orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum,
 order_processing_fee, restaurant_reward_sum)
select
	do2.restaurant_id,
	dr.restaurant_name,
	dt.date as settlement_date,
	count(distinct fps.order_id) as orders_count,
	sum(fps.total_sum) as orders_total_sum,
	sum(fps.bonus_payment) as orders_bonus_payment_sum,
	sum(fps.bonus_grant) as orders_bonus_granted_sum,
	(0.25 * sum(fps.total_sum)) as order_processing_fee,
	(0.75 * sum(fps.total_sum) - sum(fps.bonus_payment)) as restaurant_reward_sum
 from dds.fct_product_sales fps
 join dds.dm_orders do2 on fps.order_id=do2.id
 	and do2.order_status = 'CLOSED'
 join dds.dm_timestamps dt on dt.id=do2.timestamp_id
 join dds.dm_products dp on fps.product_id=dp.id
 	and dp.active_to >= current_date
 join dds.dm_restaurants dr on do2.restaurant_id=dr.id
 	and dp.restaurant_id=dr.id
 	and dr.active_to >= current_date
 group by do2.restaurant_id,
 		  dr.restaurant_name,
 		  dt.date
on conflict (restaurant_id, settlement_date)
do update
set
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
;
--select * from cdm.dm_settlement_report;




--stg project
drop table if exists stg.api_restaurants;
create table if not exists stg.api_restaurants (
	id serial primary key,
	object_id varchar not null,
	object_value text not null,
	update_ts timestamp not null,
	CONSTRAINT api_restaurants_object_id_key UNIQUE (object_id)
);

drop table if exists stg.api_couriers;
create table if not exists stg.api_couriers (
	id serial primary key,
	object_id varchar not null,
	object_value text not null,
	update_ts timestamp not null,
	CONSTRAINT api_couriers_object_id_key UNIQUE (object_id)
);

drop table if exists stg.api_deliveries;
create table if not exists stg.api_deliveries (
	id serial primary key,
	object_id varchar not null,
	object_value text not null,
	update_ts timestamp not null,
	CONSTRAINT api_deliveries_object_id_key UNIQUE (object_id)
);

--dds project
drop table if exists dds.fct_deliveries;
drop table if exists dds.dm_couriers;
CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id INT4 NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL,
    active_from TIMESTAMP NOT NULL,
    active_to TIMESTAMP NOT null
);

CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
    id serial NOT NULL PRIMARY KEY,
    order_id integer NOT NULL,
    order_ts TIMESTAMP NOT NULL,
    delivery_id varchar NOT NULL,
    courier_id integer NOT NULL,
    address TEXT NOT NULL,
    delivery_ts TIMESTAMP NOT NULL,
    sum NUMERIC(14, 2) NOT null,
    tip_sum NUMERIC(14, 2) NOT null,
    rate smallint NOT null,
    CONSTRAINT fct_deliveries_delivery_id_key UNIQUE (delivery_id)
);
alter table dds.fct_deliveries add constraint
fct_deliveries_order_id_fk foreign key (order_id) references dds.dm_orders (id);
alter table dds.fct_deliveries add constraint
fct_deliveries_courier_id_fk foreign key (courier_id) references dds.dm_couriers (id);


--cdm project
drop table if exists cdm.dm_courier_ledger;
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger(
    id                      SERIAL,
    courier_id              INTEGER         NOT NULL,
    courier_name            VARCHAR         NOT NULL,
    settlement_year         INTEGER         NOT NULL,
    settlement_month        INTEGER         NOT NULL,
    orders_count            INTEGER         NOT NULL DEFAULT 0,
    orders_total_sum        NUMERIC(14, 2)  NOT NULL DEFAULT 0,
    rate_avg                NUMERIC(3, 2)   NOT NULL DEFAULT 0,
    order_processing_fee    NUMERIC(14,2)   NOT NULL DEFAULT 0,
    courier_order_sum       NUMERIC(14,2)   NOT NULL DEFAULT 0,
    courier_tips_sum        NUMERIC(14,2)   NOT NULL DEFAULT 0,
    courier_reward_sum      NUMERIC(14, 2)  NOT NULL DEFAULT 0,
    CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id),
    CONSTRAINT unique_dm_courier_ledger UNIQUE (courier_id, settlement_year, settlement_month),
    CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (settlement_year BETWEEN 2022 AND 29999),
    CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (settlement_month BETWEEN 1 AND 12),
    CONSTRAINT dm_courier_ledger_orders_count_check CHECK (orders_count >= 0),
    CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK (orders_total_sum BETWEEN 0 AND 999999999999.99),
    CONSTRAINT dm_courier_ledger_rate_avg_check CHECK (rate_avg BETWEEN 0 AND 5.00),
    CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK (order_processing_fee BETWEEN 0 AND 999999999999.99),
    CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK (courier_order_sum BETWEEN 0 AND 999999999999.99),
    CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK (courier_tips_sum BETWEEN 0 AND 999999999999.99),
    CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK (courier_reward_sum BETWEEN 0 AND 999999999999.99)
);








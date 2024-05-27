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
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

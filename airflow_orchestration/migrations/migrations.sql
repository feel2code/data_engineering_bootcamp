alter table staging.user_order_log add status varchar(8) default 'shipped';
alter table mart.f_sales add status varchar(8);

create table if not exists mart.f_customer_retention (
	new_customers_count bigint not null,
	returning_customers_count bigint not null,
	refunded_customer_count bigint not null,
	period_name varchar(6),
	period_id smallint not null,
	item_id bigint not null,
	new_customers_revenue bigint not null,
	returning_customers_revenue bigint not null,
	customers_refunded bigint not null
);
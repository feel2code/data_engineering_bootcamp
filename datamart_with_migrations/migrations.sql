-- shipping_country_rates
drop table if exists public.shipping_country_rates;
create table public.shipping_country_rates (
	id serial,
	shipping_country text,
	shipping_country_base_rate numeric(14,3),
	primary key (id)
);

-- shipping_agreement
drop table if exists public.shipping_agreement;
create table public.shipping_agreement (
	agreement_id serial,
	agreement_number text,
	agreement_rate numeric(14,2),
	agreement_commission numeric(14,2),
	primary key (agreement_id)
);

-- shipping_transfer
drop table if exists public.shipping_transfer;
create table public.shipping_transfer (
	id serial,
	transfer_type text,
	transfer_model text,
	shipping_transfer_rate numeric(14,3),
	primary key (id)
);

-- shipping_info
drop table if exists public.shipping_info;
create table public.shipping_info (
	shipping_id bigint,
	shipping_plan_datetime timestamp,
	payment_amount float4,
	vendor_id bigint,
	shipping_transfer_id bigint,
	shipping_agreement_id bigint,
	shipping_country_rate_id bigint,
	FOREIGN KEY (shipping_transfer_id) REFERENCES shipping_transfer(id) ON UPDATE CASCADE,
	FOREIGN KEY (shipping_agreement_id) REFERENCES shipping_agreement(agreement_id) ON UPDATE CASCADE,
	FOREIGN KEY (shipping_country_rate_id) REFERENCES shipping_country_rates(id) ON UPDATE CASCADE
);

-- shipping_status
drop table if exists public.shipping_status;
create table public.shipping_status (
	shipping_id bigint,
	status text,
	state text,
	shipping_start_fact_datetime timestamp,
	shipping_end_fact_datetime timestamp,
	primary key (shipping_id)
);

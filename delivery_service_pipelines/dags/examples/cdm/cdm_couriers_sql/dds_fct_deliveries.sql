insert into dds.fct_deliveries (
	order_id,
	order_ts,
	delivery_id,
	courier_id,
	address,
	delivery_ts,
	sum,
	tip_sum,
	rate
)
select
	do2.id,
	raw.order_ts,
	raw.delivery_id,
	dc.id,
	raw.address,
	raw.delivery_ts,
	raw.sum,
	raw.tip_sum,
	raw.rate
  from
    (
      select
	      object_id as order_id,
	      (object_value::JSON->>'order_ts')::date as order_ts,
	      (object_value::JSON->>'delivery_id') as delivery_id,
	      (object_value::JSON->>'courier_id') as courier_id,
	      (object_value::JSON->>'address') as address,
	      (object_value::JSON->>'delivery_ts')::date as delivery_ts,
	      (object_value::JSON->>'sum')::numeric as sum,
	      (object_value::JSON->>'tip_sum')::numeric as tip_sum,
	      (object_value::JSON->>'rate')::smallint  as rate
        FROM stg.api_deliveries
    ) as raw
    join dds.dm_couriers dc on dc.courier_id=raw.courier_id
    join dds.dm_orders do2 on do2.order_key=raw.order_id
        on conflict (delivery_id) do nothing;

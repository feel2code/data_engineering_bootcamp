insert into analysis.tmp_rfm_recency
  select
        user_id,
        ntile(5) over (partition by count(*)/5 order by last_order_dt asc) as recency
	from (
	  select u.id as user_id, coalesce(paid_orders.last_order_dt, to_timestamp('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')) as last_order_dt
	  from (
		select o.user_id, max(order_ts) as last_order_dt
		  from analysis.orders o
		  left join analysis.orderstatuses o2 on o2.id = o.status
		  where o2.key = 'Closed'
		  group by o.user_id
	  ) as paid_orders
	  right join analysis.users u on u.id = paid_orders.user_id
	  order by last_order_dt desc
	  ) as recent_orders
  group by user_id, last_order_dt
  order by recency desc;
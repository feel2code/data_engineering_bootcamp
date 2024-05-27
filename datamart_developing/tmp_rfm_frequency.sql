insert into analysis.tmp_rfm_frequency
  select
        user_id,
        ntile(5) over (partition by count(*)/5 order by orders_count asc) as frequency
  from (
	select u.id as user_id, coalesce(paid_orders.order_count, 0) as orders_count
	from (
	  select o.user_id, count(*) as order_count
		from analysis.orders o
		left join analysis.orderstatuses o2 on o2.id = o.status
		where o2.key = 'Closed'
		group by o.user_id
	) as paid_orders
	right join analysis.users u on u.id = paid_orders.user_id
	order by orders_count desc
	) as orders_counted
	group by user_id, orders_count
	order by frequency desc;
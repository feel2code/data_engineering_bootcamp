insert into analysis.tmp_rfm_monetary_value
  select
        user_id,
        ntile(5) over (partition by count(*)/5 order by payments_sum asc) as monetary_value
  from (
	select u.id as user_id, coalesce(paid_orders.order_sum, 0) as payments_sum
	from (
	  select o.user_id, sum(o.payment) as order_sum
		from analysis.orders o
		left join analysis.orderstatuses o2 on o2.id = o.status
		where o2.key = 'Closed'
		group by o.user_id
	) as paid_orders
	right join analysis.users u on u.id = paid_orders.user_id
	order by payments_sum desc
	) as orders_by_payments
  group by user_id, payments_sum
  order by monetary_value desc;
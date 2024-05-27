create or replace view analysis.Orders as (
  select o.order_id, o.order_ts, o.user_id, o.bonus_payment, o.payment, o.cost, o.bonus_grant, s.status_id as status
  from production.Orders o
  join (
	select o1.order_id, o1.status_id
		from production.orderstatuslog o1
		join (select order_id, max(dttm) as dttm from production.orderstatuslog o2 group by o2.order_id) as o3 on o3.order_id=o1.order_id and o3.dttm=o1.dttm
	) as s on o.order_id=s.order_id
);
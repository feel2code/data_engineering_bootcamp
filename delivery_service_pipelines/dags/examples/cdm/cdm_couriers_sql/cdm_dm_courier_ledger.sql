with base as (
select
fd.courier_id,
dc.courier_name,
dt."year" as settlement_year,
dt."month" as settlement_month,
count(fd.delivery_id) as orders_count,
sum(fd.sum) as orders_total_sum,
avg(rate) as rate_avg,
sum(fd.sum) * 0.25 as order_processing_fee,
sum(tip_sum) as courier_tips_sum
from
dds.fct_deliveries fd
join dds.dm_couriers dc on dc.id=fd.courier_id
join dds.dm_orders do2 on do2.id=fd.order_id
join dds.dm_timestamps dt on dt.id=do2.timestamp_id
group by fd.courier_id, dc.courier_name, dt."year", dt."month"
),
rate_base as (
	select *, (
		CASE
		   WHEN rate_avg < 4.0 THEN
		       CASE WHEN 0.05 * orders_total_sum >= 100 THEN 0.05 * orders_total_sum ELSE 100 end
		   WHEN rate_avg >= 4.0 and rate_avg < 4.5 THEN
		       CASE WHEN 0.07 * orders_total_sum >= 150 THEN 0.07 * orders_total_sum ELSE 150 END
		   WHEN rate_avg >= 4.5 and rate_avg < 4.9 THEN
		       CASE WHEN 0.08 * orders_total_sum >= 175 THEN 0.08 * orders_total_sum ELSE 175 END
		   WHEN rate_avg >= 4.9 THEN
		       CASE WHEN 0.1 * orders_total_sum >= 200 THEN 0.1 * orders_total_sum ELSE 200 END
		   end) as courier_order_sum
	from base
)
INSERT INTO cdm.dm_courier_ledger(
  courier_id,
  courier_name,
  settlement_year,
  settlement_month,
  orders_count,
  orders_total_sum,
  rate_avg,
  order_processing_fee,
  courier_order_sum,
  courier_tips_sum,
  courier_reward_sum)
select
	courier_id,
	courier_name,
	settlement_year,
	settlement_month,
	orders_count,
	orders_total_sum,
	rate_avg,
	order_processing_fee,
	courier_order_sum,
	courier_tips_sum,
	courier_order_sum + courier_tips_sum * 0.95 as courier_reward_sum
	from rate_base
         ON CONFLICT (courier_id, settlement_year, settlement_month)
         DO UPDATE SET
orders_count = excluded.orders_count,
orders_total_sum = excluded.orders_total_sum,
rate_avg = excluded.rate_avg,
order_processing_fee = excluded.order_processing_fee,
courier_order_sum = excluded.courier_order_sum,
courier_tips_sum = excluded.courier_tips_sum,
courier_reward_sum = excluded.courier_reward_sum
;

--select * from mart.f_customer_retention fcr ;
truncate mart.f_customer_retention;
insert into mart.f_customer_retention (new_customers_count,
                                       returning_customers_count,
                                       refunded_customer_count,
                                       period_name,
                                       period_id,
                                       item_id,
                                       new_customers_revenue,
                                       returning_customers_revenue,
                                       customers_refunded)
select count(case when orders = 1 then 1 else 0 end)               as new_customers_count,
       count(case when orders > 1 then 1 else 0 end)               as returning_customers_count,
       count(case when refunded_orders <> 1 then 1 else 0 end)     as refunded_customer_count,
       'weekly'                                                    as period_name,
       week_of_year                                                as period_id,
       item_id,
       sum(case when orders = 1 then revenue else 0 end)           as new_customers_revenue,
       sum(case when orders > 1 then revenue else 0 end)           as returning_customers_revenue,
       sum(case when refunded_orders <> 1 then revenue else 0 end) as customers_refunded
from (select dc.week_of_year,
             customer_id,
             item_id,
             count(*)                                             as orders,
             sum(case when status = 'refunded' then 1 else 0 end) as refunded_orders,
             sum(payment_amount)                                  as revenue
      from mart.f_sales fs2
               join mart.d_calendar dc ON dc.date_id = fs2.date_id
      group by dc.week_of_year, customer_id, item_id) as orders_aggregated
group by week_of_year, item_id
order by week_of_year asc;
--shipping_datamart
drop view if exists shipping_datamart;
create view shipping_datamart as
select si.shipping_id, vendor_id, transfer_type,
	   age(shipping_end_fact_datetime, shipping_start_fact_datetime) as full_day_at_shipping,
	   (shipping_end_fact_datetime > shipping_plan_datetime)::int as is_delay,
	   (status = 'finished')::int as is_shipping_finish,
	   shipping_end_fact_datetime, shipping_plan_datetime,
	   (case
		   when (shipping_end_fact_datetime > shipping_plan_datetime)
		   then date_part('day', age(shipping_end_fact_datetime, shipping_plan_datetime))
		   else 0
	   end) as delay_day_at_shipping,
	   payment_amount,
	   (payment_amount * (shipping_country_base_rate + agreement_rate + shipping_transfer_rate)) as vat,
	   (payment_amount * agreement_commission) as profit
from shipping_info si
join shipping_status ss on si.shipping_id=ss.shipping_id
join shipping_transfer st on si.shipping_transfer_id=st.id
join shipping_agreement sa on si.shipping_agreement_id=sa.agreement_id
join shipping_country_rates sc on si.shipping_country_rate_id=sc.id
order by si.shipping_id;

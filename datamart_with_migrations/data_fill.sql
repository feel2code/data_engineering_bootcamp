-- shipping_country_rates
insert into public.shipping_country_rates
(shipping_country, shipping_country_base_rate)
select distinct shipping_country, shipping_country_base_rate from public.shipping s;

-- shipping_agreement
insert into public.shipping_agreement
(agreement_id, agreement_number, agreement_rate, agreement_commission)
select vendor_agreements[1]::int, vendor_agreements[2]::text, vendor_agreements[3]::numeric(14,2), vendor_agreements[4]::numeric(14,2)
from (
	select (regexp_split_to_array(vendor_agreement_description , E'\\:+')) as vendor_agreements
	from public.shipping
	group by vendor_agreement_description
) as s
order by vendor_agreements[1]::int asc;

-- shipping_transfer
insert into public.shipping_transfer
(transfer_type, transfer_model, shipping_transfer_rate)
select shipping_transfer_description[1], shipping_transfer_description[2], shipping_transfer_rate from (
	select (regexp_split_to_array(shipping_transfer_description, E'\\:+')) as shipping_transfer_description, shipping_transfer_rate
	from public.shipping
group by shipping_transfer_description, shipping_transfer_rate) as s;

-- shipping_info
insert into public.shipping_info
(shipping_id, shipping_plan_datetime, payment_amount, vendor_id, shipping_transfer_id, shipping_agreement_id, shipping_country_rate_id)
select distinct s.shipping_id, s.shipping_plan_datetime, s.payment, s.vendor_id, st.id, sa.agreement_id, sc.id
from public.shipping s
join shipping_transfer st on s.shipping_transfer_rate=st.shipping_transfer_rate and s.shipping_transfer_description=(st.transfer_type || ':' || st.transfer_model)
join shipping_agreement sa on (regexp_split_to_array(s.vendor_agreement_description, E'\\:+'))[1]::int=sa.agreement_id
join shipping_country_rates sc on s.shipping_country=sc.shipping_country;


-- shipping_status
insert into public.shipping_status
(shipping_id, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
select shipping_id, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime
from shipping s
join (
	select shipping_id, state_datetime as shipping_start_fact_datetime
	from shipping
	where state='booked'
	) as start_state using (shipping_id)
join (
	select shipping_id, state_datetime as shipping_end_fact_datetime
	from shipping
	where state='recieved'
	) as end_state using (shipping_id)
where state_datetime=(select max(state_datetime) from shipping s2 where s2.shipping_id=s.shipping_id)
order by shipping_id;

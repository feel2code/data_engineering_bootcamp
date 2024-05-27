insert into dds.dm_couriers
            (courier_id, courier_name, active_from, active_to)
SELECT object_id,
       (object_value::JSON->> 'name') as name,
       update_ts,
       '2099-12-31 00:00:00.000'
FROM stg.api_couriers as stg_c;
update dds.dm_couriers as v
set active_to = now()
where NOT EXISTS(
SELECT FROM stg.api_couriers c1
WHERE c1.object_id = v.courier_id
);

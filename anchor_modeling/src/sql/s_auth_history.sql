create table VERTICA_SCHEMA__DWH.s_auth_history
(
    hk_l_user_group_activity bigint not null
        CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES VERTICA_SCHEMA__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from  int,
    event         varchar(6),
    event_dt      timestamp,
    load_dt       datetime,
    load_src      varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_l_user_group_activity all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.s_auth_history (hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
select distinct luga.hk_l_user_group_activity,
       hu.hk_user_id as user_id_from,
       gl.event,
       gl.datetime,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__STAGING.group_log as gl
    left join VERTICA_SCHEMA__DWH.h_users as hu on gl.user_id=hu.user_id
    left join VERTICA_SCHEMA__DWH.h_groups as hg on gl.group_id = hg.group_id
    left join VERTICA_SCHEMA__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;

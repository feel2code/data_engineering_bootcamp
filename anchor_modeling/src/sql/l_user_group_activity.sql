create table VERTICA_SCHEMA__DWH.l_user_group_activity
  (
    hk_l_user_group_activity bigint primary key,
    hk_user_id       bigint not null
    CONSTRAINT fk_l_user_group_activity_user REFERENCES STV2023111332__DWH.h_users (hk_user_id),
    hk_group_id         bigint not null
    CONSTRAINT fk_l_user_group_activity_group REFERENCES STV2023111332__DWH.h_groups (hk_group_id),
    load_dt             datetime,
    load_src            varchar(20)
  )
  order by load_dt
  SEGMENTED BY hk_user_id all nodes
  PARTITION BY load_dt::date
  GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.l_user_group_activity (hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select hash(hu.hk_user_id, hg.group_id),
       hu.hk_user_id,
       hg.hk_group_id,
       now() as load_dt,
       's3'  as load_src
  from VERTICA_SCHEMA__STAGING.group_log as g
       left join VERTICA_SCHEMA__DWH.h_users as hu on g.user_id = hu.user_id
       left join VERTICA_SCHEMA__DWH.h_groups as hg on g.group_id = hg.group_id
 where hash(hu.hk_user_id, hg.group_id) not in (select hk_l_user_group_activity from VERTICA_SCHEMA__DWH.l_user_group_activity);

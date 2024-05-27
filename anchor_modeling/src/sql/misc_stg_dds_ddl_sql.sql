--staging
drop table if exists VERTICA_SCHEMA__STAGING.dialogs;
drop table if exists VERTICA_SCHEMA__STAGING.groups;
drop table if exists VERTICA_SCHEMA__STAGING.users;
create table VERTICA_SCHEMA__STAGING.users
(
    id              int primary key,
    chat_name       varchar(200),
    registration_dt timestamp,
    country         varchar(200),
    age             int
) order by id;
create table VERTICA_SCHEMA__STAGING.groups
(
    id              int primary key,
    admin_id        int,
    group_name      varchar(100),
    registration_dt timestamp,
    is_private      boolean
) order by id, admin_id
    PARTITION BY registration_dt::date
        GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);
create table VERTICA_SCHEMA__STAGING.dialogs
(
    message_id    int primary key,
    message_ts    timestamp NOT NULL,
    message_from  int       NOT NULL,
    message_to    int       NOT NULL,
    message       varchar(1000),
    message_group int       null
) order by message_id
    PARTITION BY message_ts::date
        GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);
alter table VERTICA_SCHEMA__STAGING.groups
    add constraint groups_admin_id_users_fk foreign key (admin_id) references VERTICA_SCHEMA__STAGING.users (id);
alter table VERTICA_SCHEMA__STAGING.dialogs
    add constraint dialogs_message_from_users_fk foreign key (message_from) references VERTICA_SCHEMA__STAGING.users (id);
alter table VERTICA_SCHEMA__STAGING.dialogs
    add constraint dialogs_message_to_users_fk foreign key (message_to) references VERTICA_SCHEMA__STAGING.users (id);

select *
from VERTICA_SCHEMA__STAGING.dialogs;


--checks
select count(*) as total, count(distinct id) as uniq, 'users' as dataset
from VERTICA_SCHEMA__STAGING.users
union all
select count(*) as total, count(distinct id) as uniq, 'groups' as dataset
from VERTICA_SCHEMA__STAGING.groups
union all
select count(*) as total, count(distinct message_id) as uniq, 'dialogs' as dataset
from VERTICA_SCHEMA__STAGING.dialogs;

SELECT count(hash(g.group_name)), count(distinct hash(g.group_name))
FROM VERTICA_SCHEMA__STAGING.groups g
LIMIT 10;

(SELECT min(u.registration_dt)       as datestamp,
        'earliest user registration' as info
 FROM VERTICA_SCHEMA__STAGING.users u)
UNION ALL
(SELECT max(u.registration_dt),
        'latest user registration'
 FROM VERTICA_SCHEMA__STAGING.users u)
UNION ALL
(SELECT min(g.registration_dt)    as datestamp,
        'earliest group creation' as info
 FROM VERTICA_SCHEMA__STAGING.groups g)
UNION ALL
(SELECT max(g.registration_dt),
        'latest group creation'
 FROM VERTICA_SCHEMA__STAGING.groups g)
UNION ALL
(SELECT min(d.message_ts)         as datestamp,
        'earliest dialog message' as info
 FROM VERTICA_SCHEMA__STAGING.dialogs d)
UNION ALL
(SELECT max(d.message_ts),
        'latest dialog message'
 FROM VERTICA_SCHEMA__STAGING.dialogs d);


(SELECT max(u.registration_dt) < now() as 'no future dates',
        true                           as 'no false-start dates',
        'users'                        as dataset
 FROM VERTICA_SCHEMA__STAGING.users u)
UNION ALL
(SELECT max(g.registration_dt) < now() as 'no future dates',
        true                           as 'no false-start dates',
        'groups'                       as dataset
 FROM VERTICA_SCHEMA__STAGING.groups g)
UNION ALL
(SELECT max(d.message_ts) < now() as 'no future dates',
        true                      as 'no false-start dates',
        'dialogs'                 as dataset
 FROM VERTICA_SCHEMA__STAGING.dialogs d);


SELECT count(*)
FROM VERTICA_SCHEMA__STAGING.groups AS g
         left join VERTICA_SCHEMA__STAGING.users AS u
                   ON u.id = g.admin_id
WHERE u.id is null;

(SELECT count(1), 'missing group admin info' as info
 FROM VERTICA_SCHEMA__STAGING.groups g
          left join VERTICA_SCHEMA__STAGING.users AS u ON u.id = g.admin_id
 WHERE u.id is null)
UNION ALL
(SELECT COUNT(1), 'missing sender info'
 FROM VERTICA_SCHEMA__STAGING.dialogs d
          left join VERTICA_SCHEMA__STAGING.users AS u ON u.id = d.message_from
 WHERE u.id is null)
UNION ALL
(SELECT COUNT(1), 'missing receiver info'
 FROM VERTICA_SCHEMA__STAGING.dialogs d
          left join VERTICA_SCHEMA__STAGING.users AS u ON u.id = d.message_to
 WHERE u.id is null)
UNION ALL
(SELECT COUNT(1), 'norm receiver info'
 FROM VERTICA_SCHEMA__STAGING.dialogs d
          right join VERTICA_SCHEMA__STAGING.users AS u ON u.id = d.message_to
 WHERE u.id is not null);


--dds
drop table if exists VERTICA_SCHEMA__DWH.h_users;
drop table if exists VERTICA_SCHEMA__DWH.h_groups;
drop table if exists VERTICA_SCHEMA__DWH.h_dialogs;
create table VERTICA_SCHEMA__DWH.h_users
(
    hk_user_id      bigint primary key,
    user_id         int,
    registration_dt datetime,
    load_dt         datetime,
    load_src        varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_user_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table VERTICA_SCHEMA__DWH.h_groups
(
    hk_group_id     int primary key,
    group_id        int,
    registration_dt timestamp,
    load_dt         datetime,
    load_src        varchar(20)
) order by load_dt
    SEGMENTED BY hk_group_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table VERTICA_SCHEMA__DWH.h_dialogs
(
    hk_message_id int primary key,
    message_id    int,
    message_ts    timestamp NOT NULL,
    load_dt       datetime,
    load_src      varchar(20)
) order by load_dt
    SEGMENTED BY hk_message_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


--stg to dds
INSERT INTO VERTICA_SCHEMA__DWH.h_users(hk_user_id, user_id, registration_dt, load_dt, load_src)
select hash(id) as hk_user_id,
       id       as user_id,
       registration_dt,
       now()    as load_dt,
       's3'     as load_src
from VERTICA_SCHEMA__STAGING.users
where hash(id) not in (select hk_user_id from VERTICA_SCHEMA__DWH.h_users);

INSERT INTO VERTICA_SCHEMA__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
select hash(id) as hk_group_id,
       id       as group_id,
       registration_dt,
       now()    as load_dt,
       's3'     as load_src
from VERTICA_SCHEMA__STAGING.groups
where hash(id) not in (select hk_group_id from VERTICA_SCHEMA__DWH.h_groups);

INSERT INTO VERTICA_SCHEMA__DWH.h_dialogs(hk_message_id, message_id, message_ts, load_dt, load_src)
select hash(message_id) as hk_message_id,
       message_id       as message_id,
       message_ts,
       now()            as load_dt,
       's3'             as load_src
from VERTICA_SCHEMA__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from VERTICA_SCHEMA__DWH.h_dialogs);


--dds links
drop table if exists VERTICA_SCHEMA__DWH.l_user_message;
drop table if exists VERTICA_SCHEMA__DWH.l_admins;
drop table if exists VERTICA_SCHEMA__DWH.l_groups_dialogs;
create table VERTICA_SCHEMA__DWH.l_user_message
(
    hk_l_user_message bigint primary key,
    hk_user_id        bigint not null
        CONSTRAINT fk_l_user_message_user REFERENCES VERTICA_SCHEMA__DWH.h_users (hk_user_id),
    hk_message_id     bigint not null
        CONSTRAINT fk_l_user_message_message REFERENCES VERTICA_SCHEMA__DWH.h_dialogs (hk_message_id),
    load_dt           datetime,
    load_src          varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_user_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table VERTICA_SCHEMA__DWH.l_admins
(
    hk_l_admin_id bigint primary key,
    hk_user_id    bigint not null
        CONSTRAINT fk_l_admins_user REFERENCES VERTICA_SCHEMA__DWH.h_users (hk_user_id),
    hk_group_id   bigint not null
        CONSTRAINT fk_l_admins_group REFERENCES VERTICA_SCHEMA__DWH.h_groups (hk_group_id),
    load_dt       datetime,
    load_src      varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_l_admin_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table VERTICA_SCHEMA__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs bigint primary key,
    hk_message_id       bigint not null
        CONSTRAINT fk_l_groups_dialogs_message REFERENCES VERTICA_SCHEMA__DWH.h_dialogs (hk_message_id),
    hk_group_id         bigint not null
        CONSTRAINT fk_l_groups_dialogs_group REFERENCES VERTICA_SCHEMA__DWH.h_groups (hk_group_id),
    load_dt             datetime,
    load_src            varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_l_groups_dialogs all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


--load links
INSERT INTO VERTICA_SCHEMA__DWH.l_admins(hk_l_admin_id, hk_group_id, hk_user_id, load_dt, load_src)
select hash(hg.hk_group_id, hu.hk_user_id),
       hg.hk_group_id,
       hu.hk_user_id,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__STAGING.groups as g
         left join VERTICA_SCHEMA__DWH.h_users as hu on g.admin_id = hu.user_id
         left join VERTICA_SCHEMA__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id, hu.hk_user_id) not in (select hk_l_admin_id from VERTICA_SCHEMA__DWH.l_admins);

INSERT INTO VERTICA_SCHEMA__DWH.l_user_message(hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
select hash(hu.hk_user_id, hd.hk_message_id),
       hu.hk_user_id,
       hd.hk_message_id,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__STAGING.dialogs as d
         join VERTICA_SCHEMA__DWH.h_users as hu on d.message_id = hu.user_id
         join VERTICA_SCHEMA__DWH.h_dialogs as hd on d.message_id = hd.message_id
where hash(hu.hk_user_id, hd.hk_message_id) not in (select hk_l_user_message from VERTICA_SCHEMA__DWH.l_user_message);

INSERT INTO VERTICA_SCHEMA__DWH.l_groups_dialogs(hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
select hash(hd.hk_message_id, hg.group_id),
       hd.hk_message_id,
       hg.hk_group_id,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__STAGING.dialogs as d
         left join VERTICA_SCHEMA__DWH.h_dialogs as hd on d.message_id = hd.message_id
         right join VERTICA_SCHEMA__DWH.h_groups as hg on d.message_group = hg.group_id
where hash(hd.hk_message_id, hg.group_id) not in (select hk_l_groups_dialogs from VERTICA_SCHEMA__DWH.l_groups_dialogs);


--dds satellites and load
--drop satellites
drop table if exists VERTICA_SCHEMA__DWH.s_admins;
drop table if exists VERTICA_SCHEMA__DWH.s_user_chatinfo;
drop table if exists VERTICA_SCHEMA__DWH.s_user_socdem;
drop table if exists VERTICA_SCHEMA__DWH.s_group_private_status;
drop table if exists VERTICA_SCHEMA__DWH.s_group_name;
drop table if exists VERTICA_SCHEMA__DWH.s_dialog_info;

--s_admins
create table VERTICA_SCHEMA__DWH.s_admins
(
    hk_admin_id bigint not null
        CONSTRAINT fk_s_admins_l_admins REFERENCES VERTICA_SCHEMA__DWH.l_admins (hk_l_admin_id),
    is_admin    boolean,
    admin_from  datetime,
    load_dt     datetime,
    load_src    varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_admin_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.s_admins(hk_admin_id, is_admin, admin_from, load_dt, load_src)
select la.hk_l_admin_id,
       True  as is_admin,
       hg.registration_dt,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__DWH.l_admins as la
         left join VERTICA_SCHEMA__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;

--s_user_chatinfo;
create table VERTICA_SCHEMA__DWH.s_user_chatinfo
(
    hk_user_id bigint not null
        CONSTRAINT fk_s_user_chatinfo_h_users REFERENCES VERTICA_SCHEMA__DWH.h_users (hk_user_id),
    chat_name  varchar(200),
    load_dt    datetime,
    load_src   varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_user_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.s_user_chatinfo(hk_user_id, chat_name, load_dt, load_src)
select hu.hk_user_id,
       u.chat_name as chat_name,
       now()       as load_dt,
       's3'        as load_src
from VERTICA_SCHEMA__DWH.h_users as hu
         left join VERTICA_SCHEMA__STAGING.users as u on u.id = hu.user_id;

--s_user_socdem;
create table VERTICA_SCHEMA__DWH.s_user_socdem
(
    hk_user_id bigint not null
        CONSTRAINT fk_s_user_socdem_h_users REFERENCES VERTICA_SCHEMA__DWH.h_users (hk_user_id),
    country    varchar(100),
    age        int,
    load_dt    datetime,
    load_src   varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_user_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.s_user_socdem(hk_user_id, country, age, load_dt, load_src)
select hu.hk_user_id,
       u.country as country,
       u.age     as age,
       now()     as load_dt,
       's3'      as load_src
from VERTICA_SCHEMA__DWH.h_users as hu
         left join VERTICA_SCHEMA__STAGING.users as u on u.id = hu.user_id;

--s_group_private_status;
create table VERTICA_SCHEMA__DWH.s_group_private_status
(
    hk_group_id bigint not null
        CONSTRAINT fk_s_group_private_status_h_groups REFERENCES VERTICA_SCHEMA__DWH.h_groups (hk_group_id),
    is_private  boolean,
    load_dt     datetime,
    load_src    varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_group_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.s_group_private_status (hk_group_id, is_private, load_dt, load_src)
select hg.hk_group_id,
       g.is_private,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__DWH.h_groups as hg
         left join VERTICA_SCHEMA__STAGING.groups as g on g.id = hg.group_id;

--s_group_name;
create table VERTICA_SCHEMA__DWH.s_group_name
(
    hk_group_id bigint not null
        CONSTRAINT fk_s_group_name_h_groups REFERENCES VERTICA_SCHEMA__DWH.h_groups (hk_group_id),
    group_name  varchar(200),
    load_dt     datetime,
    load_src    varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_group_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.s_group_name (hk_group_id, group_name, load_dt, load_src)
select hg.hk_group_id,
       g.group_name,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__DWH.h_groups as hg
         left join VERTICA_SCHEMA__STAGING.groups as g on g.id = hg.group_id;

--s_dialog_info;
create table VERTICA_SCHEMA__DWH.s_dialog_info
(
    hk_message_id bigint not null
        CONSTRAINT fk_s_dialog_info_h_groups REFERENCES VERTICA_SCHEMA__DWH.h_dialogs (hk_message_id),
    message       varchar(1000),
    message_from  int,
    message_to    int,
    load_dt       datetime,
    load_src      varchar(20)
)
    order by load_dt
    SEGMENTED BY hk_message_id all nodes
    PARTITION BY load_dt::date
        GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO VERTICA_SCHEMA__DWH.s_dialog_info (hk_message_id, message, message_from, message_to, load_dt, load_src)
select hd.hk_message_id,
       d.message,
       d.message_from,
       d.message_to,
       now() as load_dt,
       's3'  as load_src
from VERTICA_SCHEMA__DWH.h_dialogs as hd
         left join VERTICA_SCHEMA__STAGING.dialogs as d on d.message_id = hd.message_id;

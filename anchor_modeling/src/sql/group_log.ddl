create table VERTICA_SCHEMA__STAGING.group_log
(
    group_id        int,
    user_id         int,
    user_id_from    int null,
    event           varchar(6),
    datetime        timestamp
) order by id, group_id, user_id
    PARTITION BY datetime::date
        GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);
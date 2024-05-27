with user_group_messages as (select hk_group_id                   as hk_group_id,
                                    count(distinct hk_message_id) as cnt_users_in_group_with_messages
                             from VERTICA_SCHEMA__DWH.l_groups_dialogs
                             where hk_group_id in
                                   (select hk_group_id
                                    from VERTICA_SCHEMA__DWH.h_groups
                                    order by registration_dt
                                    limit 10)
                             group by hk_group_id),
     user_group_log as (select hk_group_id as hk_group_id, count(distinct hk_user_id) as cnt_added_users
                        from VERTICA_SCHEMA__DWH.l_user_group_activity
                        where hk_l_user_group_activity in
                              (select hk_l_user_group_activity
                               from VERTICA_SCHEMA__DWH.s_auth_history
                               where event = 'add')
                          and hk_group_id in
                              (select hk_group_id from VERTICA_SCHEMA__DWH.h_groups order by registration_dt limit 10)
                        group by hk_group_id)
select ugl.hk_group_id,
       cnt_added_users,
       cnt_users_in_group_with_messages,
       ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users AS group_conversion
from user_group_log as ugl
         left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc;

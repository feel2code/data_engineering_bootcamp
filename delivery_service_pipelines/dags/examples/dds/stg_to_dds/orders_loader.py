from logging import Logger
from typing import List, Union
from datetime import datetime

from examples.dds.dds_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OrdersObj(BaseModel):
    id: int
    user_id: int
    restaurant_id: int
    timestamp_id: int
    order_key: str
    order_status: str


class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, orders_threshold: int, limit: int) -> List[OrdersObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, object_value, date_trunc('second', update_ts) as update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": orders_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()

            cur.execute("select restaurant_id, id from dds.dm_restaurants;")
            restaurants = dict(cur.fetchall())

            cur.execute("select user_id, id from dds.dm_users;")
            users = dict(cur.fetchall())


            cur.execute("select ts, id from dds.dm_timestamps;")
            timestamps = dict(cur.fetchall())

            modeled_objs = []
            for obj in objs:
                values = str2json(obj[1])

                id = obj[0]
                user_id = users[values['user']['id']]
                restaurant_id = restaurants[values['restaurant']['id']]
                timestamp_id = timestamps[obj[2]]
                order_key = values['_id']
                order_status = values['final_status']
                modeled_obj = OrdersObj(
                    id=id,
                    user_id=user_id,
                    restaurant_id=restaurant_id,
                    timestamp_id=timestamp_id,
                    order_key=order_key,
                    order_status=order_status
                )
                modeled_objs.append(modeled_obj)
        return modeled_objs


class OrdersDestRepository:

    def insert_orders(self, conn: Connection, orders: OrdersObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_key)s, %(order_status)s)
                ;""",
                {
                    "user_id": orders.user_id,
                    "restaurant_id": orders.restaurant_id,
                    "timestamp_id": orders.timestamp_id,
                    "order_key": orders.order_key,
                    "order_status": orders.order_status,
                },
            )


class OrdersLoader:
    WF_KEY = "example_orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = OrdersOriginRepository(pg_dest)
        self.dds = OrdersDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for orders in load_queue:
                self.dds.insert_orders(conn, orders)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

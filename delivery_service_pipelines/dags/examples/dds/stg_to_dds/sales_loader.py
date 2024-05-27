from logging import Logger
from typing import List, Union
from datetime import datetime

from examples.dds.dds_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import str2json, json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class SalesObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class SalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_sales(self, sales_threshold: int, limit: int) -> List[SalesObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, object_value, date_trunc('second', update_ts) as update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": sales_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()

            cur.execute("select order_key, id from dds.dm_orders;")
            orders = dict(cur.fetchall())

            cur.execute("select product_id, id from dds.dm_products;")
            products = dict(cur.fetchall())

            cur.execute(
                """
                select
	            replace((event_value::json -> 'order_id')::varchar, '"','') as order_id,
	            (event_value::json -> 'product_payments')::text as product_payments
                from stg.bonussystem_events
                where event_type = 'bonus_transaction';
                """
            )
            bonuses = dict(cur.fetchall())

            modeled_objs = []
            for obj in objs:
                values = str2json(obj[1])

                if values['final_status'] == 'CANCELLED':
                    continue

                id = obj[0]
                order_id = orders[values['_id']]
                total_sum = values['cost']
                order_items = values['order_items']
                for item in order_items:
                    product_id = products[item['id']]
                    count = item['quantity']
                    price = item['price']
                    total_sum = count * price

                    try:
                        bonuses_order = str2json(bonuses[values['_id']])
                        for el in bonuses_order:
                            if el['product_id'] == item['id']:
                                bonus_payment = el['bonus_payment']
                                bonus_grant = el['bonus_grant']
                    except KeyError:
                        print(
                            f"passing not used bonuses id: {values['_id']}"
                        )
                        bonus_payment = 0
                        bonus_grant = 0

                    modeled_obj = SalesObj(
                        id=id,
                        product_id=product_id,
                        order_id=order_id,
                        count=count,
                        price=price,
                        total_sum=total_sum,
                        bonus_payment=bonus_payment,
                        bonus_grant=bonus_grant
                    )
                    modeled_objs.append(modeled_obj)
        return modeled_objs


class SalesDestRepository:

    def insert_sales(self, conn: Connection, sales: SalesObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.fct_product_sales
                (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                ;""",
                {
                    "product_id": sales.product_id,
                    "order_id": sales.order_id,
                    "count": sales.count,
                    "price": sales.price,
                    "total_sum": sales.total_sum,
                    "bonus_payment": sales.bonus_payment,
                    "bonus_grant": sales.bonus_grant
                },
            )


class SalesLoader:
    WF_KEY = "example_sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = SalesOriginRepository(pg_dest)
        self.dds = SalesDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_sales(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0,
                                        workflow_key=self.WF_KEY,
                                        workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for sales in load_queue:
                self.dds.insert_sales(conn, sales)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                [t.id for t in load_queue]
            )
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn,
                                                  wf_setting.workflow_key,
                                                  wf_setting_json)

            self.log.info(
                f"Loaded on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}"
            )

import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from airflow import AirflowException
from airflow.utils.task_group import TaskGroup
from bson.json_util import dumps, loads
import logging
from airflow.models.variable import Variable
from airflow.operators.postgres_operator import PostgresOperator
import os
import time
from lib.dict_util import str2json, json2str

nickname = Variable.get("X-Nickname")
cohort = Variable.get("X-Cohort")
api_key = Variable.get("X-API-Key" )
api_url = Variable.get("API-URL")

postgres_conn_id = "PG_WAREHOUSE_CONNECTION"

headers = {
        "X-API-KEY": api_key,
        "X-Nickname": nickname,
        "X-Cohort": str(cohort)
    }


def api_load_data_to_stg(url, headers, postgres_conn_id, endpoint, key):
    offset, response_dict = 0, True  # for the first batch
    while response_dict:
        limits = f"?sort_field={key}&sort_direction=asc&limit=50&offset={offset}"
        response = requests.get(url + endpoint + limits, headers=headers)
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        table_mapper = {
            '/restaurants': 'stg.api_restaurants',
            '/couriers': 'stg.api_couriers',
            '/deliveries': 'stg.api_deliveries'
        }
        table = table_mapper[endpoint]


        parsing_date = datetime.utcnow()
        workflow_settings ={
            "last_loaded_ts": parsing_date
        }
        wf_setting_json = json2str(workflow_settings)

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                response_dict = json.loads(response.content)
                if response.status_code == 200 and response_dict:
                    logging.info('Got OK status code.')
                    for el in response_dict:
                        obj_id = el[key]
                        obj_value = str(el).replace("'", '"')
                        logging.info(f"Inserting to stg el: {el}")
                        cur.execute(
                            """INSERT INTO %s (object_id, object_value, update_ts) VALUES ('%s','%s','%s')
                            ON CONFLICT (object_id) DO UPDATE
                            SET
                            object_value=EXCLUDED.object_value,
                            update_ts=EXCLUDED.update_ts
                            ;""" % (
                                table, obj_id, obj_value, parsing_date)
                        )
                        conn.commit()
                    cur.execute(
                        """INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                        VALUES ('%s', '%s')
                        ON CONFLICT (workflow_key) DO UPDATE
                        SET workflow_settings = EXCLUDED.workflow_settings
                        ;""" % (
                        table, wf_setting_json)
                    )
                    conn.commit()
                elif response.status_code != 200:
                    raise AirflowException(
                        f'Failed with API error, got {response.status_code}'
                    )
                elif not response_dict:
                    logging.info('Response empty, finishing batching...')
                else:
                    raise AirflowException(
                        'Failed due to unexpected behavior!'
                    )
        time.sleep(1)
        offset += 50


default_args = {
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id="dm_couriers",
         default_args=default_args,
         start_date=datetime(2015, 1, 1),
         schedule_interval='@daily',
         tags=["couriers"],
         catchup=False
         ) as dag:

    defaults_api_dict = {
        'url': api_url,
        'headers': headers
    }

    with TaskGroup("api_to_stg") as group1:
        task1 = PythonOperator(
            task_id="api_restaurants",
            python_callable=api_load_data_to_stg,
            provide_context=True,
            op_kwargs={**defaults_api_dict,
                       'postgres_conn_id': postgres_conn_id,
                       'endpoint': '/restaurants',
                       'key': '_id'}
        )
        task2 = PythonOperator(
            task_id="api_couriers",
            python_callable=api_load_data_to_stg,
            provide_context=True,
            op_kwargs={**defaults_api_dict,
                       'postgres_conn_id': postgres_conn_id,
                       'endpoint': '/couriers',
                       'key': '_id'}
        )
        task3 = PythonOperator(
            task_id="api_deliveries",
            python_callable=api_load_data_to_stg,
            provide_context=True,
            op_kwargs={**defaults_api_dict,
                       'postgres_conn_id': postgres_conn_id,
                       'endpoint': '/deliveries',
                       'key': 'order_id'}
        )
        [task1, task2, task3]

    with TaskGroup("stg_to_dds") as group2:
            task4 = PostgresOperator(
                task_id="dm_couriers",
                postgres_conn_id=postgres_conn_id,
                sql='cdm_couriers_sql/dds_dm_couriers.sql',
            )
            task5 = PostgresOperator(
                    task_id="dm_restaurants",
                    postgres_conn_id=postgres_conn_id,
                    sql='cdm_couriers_sql/dds_dm_restaurants.sql',
                )
            task6 = PostgresOperator(
                    task_id="fct_deliveries",
                    postgres_conn_id=postgres_conn_id,
                    sql='cdm_couriers_sql/dds_fct_deliveries.sql',
            )
            [task4 >> task5] >> task6

    task7 = PostgresOperator(
        task_id="final_cdm_dm_courier_ledger",
        postgres_conn_id=postgres_conn_id,
        sql='cdm_couriers_sql/cdm_dm_courier_ledger.sql'
    )

group1 >> group2 >> task7

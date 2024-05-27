import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from utils import format_date, log_message, vertica_execute


@dag(
    dag_id="datamart_etl",
    schedule_interval="30 0 * * *",
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    catchup=True,
    tags=["transactions", "datamart", "vertica"],
)
def datamart_etl():
    """
    Working with Vertica DB
    Updates currencies and transactions, prepares final datamart for dashboards in Metabase.
    """
    QUERY_FILE = "/lessons/dags/update_datamart.sql"
    VERTICA_SCHEMA = Variable.get("VERTICA_SCHEMA")

    @task(task_id="update_datamart")
    def update_datamart(yesterday_ds=None, ds=None):
        update_date, load_date = format_date(ds), format_date(yesterday_ds)
        delete_query = f"""
                DELETE FROM {VERTICA_SCHEMA}__DWH.global_metrics
                WHERE date_update::date = (%s);
            """
        try:
            vertica_execute(delete_query, (update_date,), "execute")
        except Exception as e:
            error_msg = "Error while delete datamarts data"
            log_message(error_msg, e)

        with open(QUERY_FILE, "r", encoding="utf-8") as file:
            upload_query = file.read()
        try:
            vertica_execute(
                upload_query,
                (
                    update_date,
                    load_date,
                ),
                "execute",
            )
        except Exception as e:
            error_msg = "Error while upload datamarts data"
            log_message(error_msg, e)

    update_datamart()


D = datamart_etl()

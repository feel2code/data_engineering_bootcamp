import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from utils import (format_date, load_from_postgres_and_convert_to_df,
                   vertica_execute)


@dag(
    dag_id="staging_etl",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2022, 10, 1),
    catchup=True,
    tags=["transactions", "stg", "postgres", "vertica"],
)
def staging_etl():
    """
    Working with Vertica DB
    Updates currencies and transactions, prepares final datamart for dashboards in Metabase.
    """
    VERTICA_SCHEMA = Variable.get("VERTICA_SCHEMA")
    POSTGRES_SCHEMA = Variable.get("POSTGRES_SCHEMA")

    with TaskGroup("update_transactions_currencies") as update_transactions_currencies:

        @task(task_id="update_currencies")
        def update_currencies():
            currencies = load_from_postgres_and_convert_to_df(
                f"select * from {POSTGRES_SCHEMA}.currencies;"
            )
            vert_delete_query = f"""
                TRUNCATE TABLE {VERTICA_SCHEMA}__STAGING.currencies;
                """
            vert_update_query = f"""
                COPY {VERTICA_SCHEMA}__STAGING.currencies
                (date_update, currency_code, currency_code_with, currency_with_div)
                FROM STDIN
                DELIMITER ';';
                """
            vertica_execute(vert_delete_query, "", "execute")
            vertica_execute(
                vert_update_query,
                currencies.to_csv(sep=";", index=False, header=False),
                "copy",
            )

        @task(task_id="update_transactions")
        def update_transactions(yesterday_ds=None):
            load_date = format_date(yesterday_ds)
            transactions = load_from_postgres_and_convert_to_df(
                f"""select * from {POSTGRES_SCHEMA}.transactions
                    where transaction_dt::date = '{load_date}';"""
            )
            vert_delete_query = f"""DELETE FROM {VERTICA_SCHEMA}__STAGING.transactions
                WHERE transaction_dt::date = (%s);"""
            vert_update_query = f"""
                    COPY {VERTICA_SCHEMA}__STAGING.transactions (
                        operation_id,
                        account_number_from,
                        account_number_to,
                        currency_code,
                        country,
                        status,
                        transaction_type,
                        amount,
                        transaction_dt
                    )
                    FROM STDIN
                    DELIMITER ';';
                """

            vertica_execute(vert_delete_query, (load_date,), "execute")
            vertica_execute(
                vert_update_query,
                transactions.to_csv(sep=";", index=False, header=False),
                "copy",
            )

        [update_currencies(), update_transactions()]

    [update_transactions_currencies]


D = staging_etl()

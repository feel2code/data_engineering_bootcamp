from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from airflow.models import Variable

import boto3
import pendulum
import vertica_python


AWS_ACCESS_KEY_ID = Variable.get('s3_key')
AWS_SECRET_ACCESS_KEY = Variable.get('s3_access_key')
BUCKET = Variable.get('s3_bucket')
VERTICA_SCHEMA = Variable.get('vertica_schema')


conn_info = {
    'host': Variable.get('vertica_host'),
    'port': Variable.get('vertica_port'),
    'user': Variable.get('vertica_login'),
    'password': Variable.get('vertica_password'),
    'database': Variable.get('vertica_db'),
    'autocommit': True
}


def fetch_s3_files(bucket: str, keys: list):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    for key in keys:
        s3_client.download_file(
            Bucket=bucket,
            Key=key,
            Filename=f'/data/{key}'
        )


bash_command_tmpl = """
files={{ params.files }}
for file in "${files[@]}"; do head -n 10 "$file" ; done"""


def load_table(table: str, conn_info: dict):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"""copy {VERTICA_SCHEMA}__STAGING.{table}
        from local '/data/{table}.csv'
        delimiter ',';""")
        print(f"Table {table} processed")



@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_get_data():
    bucket_files = ['users.csv', 'groups.csv', 'dialogs.csv', 'group_log.csv']

    task1 = PythonOperator(
        task_id='fetch_files',
        python_callable=fetch_s3_files,
        op_kwargs={'bucket': BUCKET, 'keys': bucket_files},
    )
    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={
            'files': str(
                tuple(
                    [f"/data/{f}" for f in bucket_files]
                )
            ).replace(",", " ").replace("'", '"')
        }
    )

    @task
    def load_dialogs():
        load_table('dialogs', conn_info)

    @task
    def load_groups():
        load_table('groups', conn_info)

    @task
    def load_users():
        load_table('users', conn_info)

    @task
    def load_group_log():
        load_table('group_log', conn_info)


    load_dialogs = load_dialogs()
    load_groups = load_groups()
    load_users = load_users()
    load_group_log = load_group_log()

    [load_dialogs, load_groups, load_users] >> load_group_log


dag = sprint6_dag_get_data()

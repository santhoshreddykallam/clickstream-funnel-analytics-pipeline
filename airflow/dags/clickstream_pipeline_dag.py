from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'santhosh',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

FILENAMES = [
    '2019-Oct.csv',
    '2019-Nov.csv',
    '2019-Dec.csv',
    '2020-Jan.csv',
    '2020-Feb.csv',
]

PROJECT_PATH = '/opt/airflow/project'

for filename in FILENAMES:
    month = filename.replace('.csv', '')
    dag_id = f'clickstream_pipeline_{month.replace("-", "_")}'

    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f'Clickstream funnel pipeline for {month}',
        schedule_interval=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['clickstream', 'funnel', 'pipeline'],
    ) as dag:

        transform = BashOperator(
            task_id='run_transform',
            bash_command=f'cd {PROJECT_PATH} && python transform.py {filename}',
        )

        load_postgres = BashOperator(
            task_id='run_load_postgres',
            bash_command=f'cd {PROJECT_PATH} && python load_postgres.py {filename}',
        )

        transform >> load_postgres

    globals()[dag_id] = dag
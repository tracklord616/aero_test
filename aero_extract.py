import datetime as dt
from . import aero

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

url = 'URL'
credentials = {}

default_args = {
    'owner': 'aero',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=2)
}

with DAG(
    'aero',
    default_args=default_args,
    description='stream data from external API to aero',
    schedule='0 0,12 * * *',
    start_date=dt.datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['aero', 'test']
) as dag:

    start = EmptyOperator(
        task_id='start',
    )

    @task(task_id='aero_extract', 
          trigger_rule='all_done')
    def aero_extract(url=url,
                     credentials=credentials):
        try:
            aero.etl_pipeline(url=url,
                              credentials=credentials)
        except Exception as e:
            raise Exception(f'task error: {e}')

    finish = EmptyOperator(
        task_id='finish'
    )

    start >> [
        aero_extract() 
    ] >> finish

import pendulum

from airflow.sdk import dag, task

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

@dag(
    dag_id="mydag",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=pendulum.duration(minutes=60),
)

def mydag_steps():
    @task
    def temp1 = 
dag = 
from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id='dags_trigger_dag_run_operator',
    schedule = "30 6 * * *", 
    start_date = pendulum.datetime(2024, 12, 24, tz='Asia/Seoul'),
    catchup=False
) as dag:
    start_task = BashOperator(
        task_id='start_task',
        bash_command='echo "start task"'
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='dags_python_operator',
        trigger_run_id=None,
        execution_date='{{data_interval_start}}',
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed']

    )

    start_task >> trigger_dag_task
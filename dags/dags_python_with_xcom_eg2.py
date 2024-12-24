from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id='dags_python_with_xcom_eg1',
    schedule = "30 6 * * *", 
    start_date = pendulum.datetime(2024, 12, 24, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    @task(task_id='python_xcom_push_by_return')
    def x_com_push_result(**kwargs):
        return 'success'

    @task(task_id="python_xcom_pull_1")
    def xcom_pull_1(**kwargs):
        ti=kwargs['ti']
        value1=ti.xcom_pull(task_ids='python_xcom_push_by_return')
        print('xcom_pull 메서드로 직접 찾은 리턴값 :' +value1)

    x_com_push_result() >> xcom_pull_1()
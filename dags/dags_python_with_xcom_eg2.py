from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id='dags_python_with_xcom_eg2',
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

    @task(task_id="python_xcom_pull_2")
    def xcom_pull_2(status, **kwargs):
        print('함수에서 나온값', status)


    python_x_com_push_result= x_com_push_result()
    xcom_pull_2(python_x_com_push_result)
    python_x_com_push_result >> xcom_pull_1()

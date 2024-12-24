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
    
    @task(task_id='python_xcom_push_task1')
    def x_com_push_1(**kwargs):
        ti=kwargs['ti']
        ti.xcom_push(key='result1', value='python_xcom_push_task1_value_1')
        ti.xcom_push(key='result2', value=[1,2,3,4])


    @task(task_id='python_xcom_push_task2')
    def x_com_push_2(**kwargs):
        ti=kwargs['ti']
        ti.xcom_push(key='result1', value='python_xcom_push_task2_value_1')
        ti.xcom_push(key='result2', value=[10,20,30,40])


    @task(task_id="python_xcom_pull_task1")
    def xcom_pull_1(**kwargs):
        ti=kwargs['ti']
        value1=ti.xcom_pull(key='result1')
        value2=ti.xcom_pull(key='result2')
        value1_1=ti.xcom_pull(key='result1', task_ids='python_xcom_push_task2')
        value1_2=ti.xcom_pull(key='result2', task_ids='python_xcom_push_task2')
        value2_1=ti.xcom_pull(key='result1', task_ids='python_xcom_push_task2')
        value2_2=ti.xcom_pull(key='result2', task_ids='python_xcom_push_task2')
        print('암묵적', value1)
        print('암묵적', value2)
        print('명시적', value1_1)
        print('명시적', value1_2)
        print('명시적', value2_1)
        print('명시적', value2_2)

    
    x_com_push_1 >> x_com_push_2 >> xcom_pull_1
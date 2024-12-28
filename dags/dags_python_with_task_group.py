from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup


with DAG(
    dag_id="dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2024, 12, 21, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
) as dag: 
    
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)

    @task_group(group_id='first_group')
    def group_1():
        ''' task_grouip 데커레이터를 이용한 첫번째 그룹'''

        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('첫번쨰 task Group 내 첫번쨰 task ')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={"msg":"첫번째 task group 내 두번째 task"}
        )

        inner_func1() >> inner_function2

    with TaskGroup(group_id='second_group', tooltip="두번쨰 그룹") as group_2:
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('두번쨰 task Group 내 첫번쨰 task ')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={"msg":"두번쨰 task group 내 두번째 task"}
        )

        
        inner_func1() >> inner_function2
    
    group_1() >> group_2
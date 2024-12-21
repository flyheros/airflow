from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_template",
    schedule="30 20 * * *",
    start_date=pendulum.datetime(2024, 12, 21, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
    dagrun_timeout=datetime.timedelta(minutes=60), # timeout 설정정
    # tags=["example", "example2"]
    # params={"example_key": "example_value"},  # task 에 공통적으로 넘겨줄 변수 
) as dag:

    @task(task_id="python_task_1")
    def print_context(**kwargs):
        print(kwargs)
        print('ds:'+ kwargs['ds'])
        print('ts:'+ kwargs['ts'])
        print('data_interval_start:'+ str(kwargs['data_interval_start']))
        print('data_interval_end:'+ str(kwargs['data_interval_end']))
        print('task_instance:'+ str(kwargs['ti']))


    def print_function(start_date, end_date):
        print('ds:'+ start_date)
        print('ts:'+ end_date)

    python_task_2 = PythonOperator (
        task_id='python_task_2',
        python_callable=print_function, 
        op_kwargs= {'start_date':'{{data_interval_start | ds}}', 'end_date':'{{data_interval_end | ds}}'}
    )
    
    print_context() >> python_task_2
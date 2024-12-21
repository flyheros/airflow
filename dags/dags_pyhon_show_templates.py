from airflow import DAG
import datetime
import pendulum
from airflow.decorators import task 

with DAG(
    dag_id="dags_python_show_template",
    schedule="30 20 * * *",
    start_date=pendulum.datetime(2024, 12, 10, tz="Asia/Seoul"),
    catchup=True, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
    # dagrun_timeout=datetime.timedelta(minutes=60), # timeout 설정정
    # tags=["example", "example2"]
    # params={"example_key": "example_value"},  # task 에 공통적으로 넘겨줄 변수 
) as dag:
    
    @task(task_id ='python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)
    
    show_templates()
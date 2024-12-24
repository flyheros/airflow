from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.decorators import task 

with DAG(
    dag_id="dags_python_email_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024, 12, 20, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
    dagrun_timeout=datetime.timedelta(minutes=60), # timeout 설정정
    tags=["xcom_pull", "xcom_push"]
    # params={"example_key": "example_value"},  # task 에 공통적으로 넘겨줄 변수 
) as dag:

    @task(task_id="something_task")
    def some_logic(**kwargs):
        from random import choice
        return choice(['Success1', 'Success2', 'Fail1', 'Fail2'])

    send_email = EmailOperator(
        task_id="send_email",
        to="jalim@net.co.kr",
        subject="{{ data_interval_end.in_timezone('Asia/Seoul') | ds}} some_logic 처리결과 ",
        html_content = "{{data_interval_start.in_timezone('Asia/Seoul') | ds}} 처리결과는 <br> \
                        {{ti.xcom_pull(task_ids='something_task') }} 했습니다 "
    )
     
    some_logic() >> send_email
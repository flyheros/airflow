from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg2",
    schedule="10 0 L * 6#2", # 매월 2째주 토요일 
    start_date=pendulum.datetime(2024, 12, 20, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
    dagrun_timeout=datetime.timedelta(minutes=60), # timeout 설정정
    tags=["macro.dateutil.relativedelta"]
    # params={"example_key": "example_value"},  # task 에 공통적으로 넘겨줄 변수 
) as dag:
    
    # START_DATE : 2주전 월요일, END_DATE : 2주전 토요일
    bash_t2 = BashOperator(
        task_id="bash_t2",  # 화면에 나타나는 것것
        env= {'START_DATE': '{{data_interval_start.in_timezone("Asia/Seoul")  + macro.dateutil.relativedelta.relativedelta(days=-19)) | ds}}',  # 2주전 월요일
              'END_DATE':'{{(data_interval_end.in_timezone("Asia/Seoul")  + macro.dateutil.relativedelta.relativedelta(days=-14)) | ds }}'}, # 2주전 토요일

        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE : $END_DATE"',
    )

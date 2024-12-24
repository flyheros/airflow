from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_xcom_eg",
    schedule="30 20 * * *",
    start_date=pendulum.datetime(2024, 12, 20, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
    # dagrun_timeout=datetime.timedelta(minutes=60), # timeout 설정정
    # tags=["example", "example2"]
    params={"ti.xcom_push": "ti.xcom_pull"},  # task 에 공통적으로 넘겨줄 변수 
) as dag:

    bash_push = BashOperator(
        task_id='bash_push',
        bash_command = "echo START &&"
        "echo XCOM_PUSHED " 
        "{{ ti.xcom_push(key='bash_pushed', value = 'first_bash_message') }} && "
        "echo COMPLETE"  #  마지막 리턴값이 저장됨
    )

    bash_pull = BashOperator(
        task_id="bash_pull",
        env = {"PUSHED_VALUE": "{{ ti.xcom_pull(key='bash_pushed' ) }}", 
               "RETURN_VALUE": "{{ ti.xcom_pull(task_ids='bash_push') }}"},
        bash_command = "echo $PUSHED_VALUE && echo $RETURN_VALUE",
        do_xcom_push=False   # bash_command의 마지막 리턴값 저장하지마라 
    )

    bash_push >> bash_pull
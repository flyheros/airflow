from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="example_bash_operator",
    schedule="30 20 * * *",
    start_date=pendulum.datetime(2024, 12, 20, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
    dagrun_timeout=datetime.timedelta(minutes=60), # timeout 설정정
    tags=["example", "example2"]
    # params={"example_key": "example_value"},  # task 에 공통적으로 넘겨줄 변수 
) as dag:
    
    # [START howto_operator_bash]
    bash_t1 = BashOperator(
        task_id="bash_t1",  # 화면에 나타나는 것것
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME"

    )
    # [END howto_operator_bash]


bash_t1>> bash_t2
from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024, 12, 20, tz="Asia/Seoul"),
    catchup=True, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
    dagrun_timeout=datetime.timedelta(minutes=60)
    # params={"example_key": "example_value"},  # task 에 공통적으로 넘겨줄 변수 
) as dag:

    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command= 'echo "data_interval_end: {{data_interval_end}}"'

    )

    
    bash_t2 = BashOperator(
        task_id="bash_t2",
        env = {
                'start_date' :'{{data_interval_start | ds}}',
                'end_date' :'{{data_interval_end | ds}}'
        },
        bash_command= 'echo $start_date && echo $end_date'
               
    )
    bash_t1 >> bash_t2
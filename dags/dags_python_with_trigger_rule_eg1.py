from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException

with DAG(
    dag_id="dags_python_with_trigger_rule_eg1",
    schedule= None,
    start_date=pendulum.datetime(2024, 12, 21, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
) as dag: 
    
    bash_upstream_1 = BashOperator(
        task_id='task_1',
        bash_command="echo task1"
    )

    @task (task_id='task_2')
    def python_2():
        raise AirflowException('task_2 Error' )

    
    @task (task_id='task_3')
    def python_3():
       print('task_3 정상 처리 ')



    # @task (task_id='task_4', trigger_rule="all_done")
    @task (task_id='task_4', trigger_rule="all_success")
    def task_4():
       print('task_4 정상 처리 ')
    
    [bash_upstream_1,  python_2(), python_3()] >> task_4()

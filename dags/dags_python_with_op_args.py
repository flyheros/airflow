from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from common.common_func import regist

with DAG(
    dag_id="dags_python_with_op_args",
    schedule="30 20 * * *",
    start_date=pendulum.datetime(2024, 12, 21, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
) as dag: 
    
    task_regist = PythonOperator(
        task_id = 'task_regist',
        python_callable= regist,
        op_args=['jalim','female', 'kr', 'Seoul']
    )
    task_regist
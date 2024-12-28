from airflow import DAG
import datetime
import pendulum
from airflow.decorators import task
from airflow.operators.python import PythonOperator


with DAG(
    dag_id="dags_branch_python_decorator",
    schedule="30 20 * * *",
    start_date=pendulum.datetime(2024, 12, 20, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
    # dagrun_timeout=datetime.timedelta(minutes=60), # timeout 설정정
    # tags=["example", "example2"]
    # params={"ti.xcom_push": "ti.xcom_pull"},  # task 에 공통적으로 넘겨줄 변수 
) as dag:
    
    @task.branch(task_id='python_branch_task')
    def select_random():
        import random
        item_ls = ['A', 'B', 'C']
        selected_item = random.choice(item_ls)
        if(selected_item=='A'):
            return 'task_a'
        elif (selected_item in ['B','C']):
            return ['task_b', 'task_c']
        
    def common_func(**kwargs):
        print(kwargs['selected'])


    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func, 
        op_kwargs={'selected':'A'}
    )

    

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func, 
        op_kwargs={'selected':'B'}
    )
    

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func, 
        op_kwargs={'selected':'C'}
    )

    select_random() >> [task_a, task_b, task_c]
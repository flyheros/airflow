from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from common.common_func import regist2

with DAG(
    dag_id="dags_python_with_trigger_rule_eg2",
    schedule= None,
    start_date=pendulum.datetime(2024, 12, 21, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
) as dag: 
    
    def select_random():
        import random
        item_list = ['A','B','C']
        selected_item = random.choice(item_list)
        if selected_item=='A':
            return 'task_a'  ## 다음 실행될 task_id를 적어준다
        elif selected_item in ['B', 'C']:
            return ['task_b', 'task_c']  ## 다음 실행될 task_id를 적어준다
        
    python_branch_task = BranchPythonOperator(
        task_id='python_branch_task',
        python_callable=select_random
    )


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


    
    task_d = BashOperator(
        task_id='task_d',
        bash_command="echo task1",
        trigger_rule="one_success"
    )

    python_branch_task >> [task_a, task_b, task_c] >> task_d
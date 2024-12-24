from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator
from airflow.decorators import task 

with DAG(
    dag_id="dags_bash_python_with_xcom",
    schedule="10 0 L * *",
    start_date=pendulum.datetime(2024, 12, 20, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
    dagrun_timeout=datetime.timedelta(minutes=60), # timeout 설정정
    tags=["xcom_pull", "xcom_push"]
    # params={"example_key": "example_value"},  # task 에 공통적으로 넘겨줄 변수 
) as dag:

    @task(task_id='python_push')
    def python_push_xcom():
        result_dict = {'status':'Excellent', 'data':[1,2,3], 'options_cnt':100}
        return result_dict

    bash_pull = BashOperator(
        task_id='bash_pull',
        env = {
            'Status': '{{ t1.xcom_pull(task_id="python_push")["status"]}}',
            'Data': '{{ t1.xcom_pull(task_id="python_push")["data"]}}',
            'Options_cnt': '{{ t1.xcom_pull(task_id="python_push")["options_cnt"]}}'
        },
        bash_command =  'echo $Status && echo $Data && echo $Options_cnt'
    )

    python_push_xcom() >> bash_pull


    bash_push = BashOperator(
        task_id='bash_push',
        bash_command = 'echo PUSH_START {{ ti.xcom_push(key="bash_pused", value=200)}}  && echo PUSH_COMPLETE'
    )
    @task(task_id='python_pull')
    def python_pull_xcom(**kwargs):
        ti=kwargs['ti']
        status_value = ti.xcom_pull(key="bash_pused")
        return_value = ti.xcom_pull(task_ids="bash_push")
        print( 'status_value: ', str(status_value))
        print( 'return_value: ', return_value)

    bash_push >> python_pull_xcom()

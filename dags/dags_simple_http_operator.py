from airflow import DAG
import datetime
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task 

with DAG(
    dag_id="dags_simple_http_operator",
    schedule=None,
    start_date=pendulum.datetime(2024, 12, 20, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
) as dag:
    
    endpoint="{{var.value.apikey_openapi_seoul_go_kr}}/json/LampScpgmtb/1/5/"
    
    tb_cycle_station_info = SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr.http',
        endpoint="{{var.value.apikey_openapi_seoul_go_kr}}/json/LampScpgmtb/1/5/",
        method='GET',
        headers  = {"Content-Type": "application/json"}
    )



    @task(task_id='print_xcom_value')
    def print_xcom_value(**kwargs):
        ti = kwargs['ti']
        endpoint_value = ti.xcom_pull(task_ids='tb_cycle_station_info')
        print(f"Endpoint from XCom: {endpoint}")



    # 로그 출력
    tb_cycle_station_info.log.info(
        "Rendered Endpoint: {{ var.value.apikey_openapi_seoul_go_kr }}/json/LampScpgmtb/1/5/"
    )

    @task(task_id='pprint_task')
    def pprint_task(**kwargs):
        ti= kwargs['ti']
        result = ti.xcom_pull(task_ids='tb_cycle_station_info')
        print('------------------------------')

        import json
        from pprint import pprint
        pprint(result)

# openapi.seoul.go.kr:8088/6f54667168666c793339467774486b/json/LampScpgmtb/1/5/

    tb_cycle_station_info >> print_xcom_value() >> pprint_task()
        
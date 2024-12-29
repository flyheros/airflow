from airflow import DAG
import datetime
import pendulum
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task 

with DAG(
    dag_id="dags_bash_operator",
    schedule=None,
    start_date=pendulum.datetime(2024, 12, 20, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
) as dag:
    tb_cycle_station_info = SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr.http',
        endpoint="{{var.value.apikey_openapi_seoul_go_kr}}/json/TrafficInfo/1/5/",
        method='GET',
        headers = {"Content-Type": "application/json",  
                    "charset":"utf-8",
                    "Accept" : "*/*"
                   }
    )

    @task(task_id='pprint_task')
    def pprint_task(**kwargs):
        ti= kwargs['ti']
        result = ti.xcom_pull(task_ids='tb_cycle_station_info')

        import pprint
        pprint.pprint(result)
        pprint.pprint('------------------------------')
        pprint.pprint(json.loads(result))


    tb_cycle_station_info >> pprint_task()
        
from airflow import DAG
import datetime
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label


with DAG(
    dag_id="dags_empty_with_edge_label",
    schedule= None,
    start_date=pendulum.datetime(2024, 12, 20, tz="Asia/Seoul"),
    catchup=False, # 누락된 일자도 모두 돌릴래? 단, 누락된 일자는 한꺼번에 실행되. 
    # params={"example_key": "example_value"},  # task 에 공통적으로 넘겨줄 변수 
) as dag:
 
    empty_1= EmptyOperator(
        task_id="empty_1"
    )
    empty_2= EmptyOperator(
        task_id="empty_2"
    )

    empty_1 >> Label('1-->2') >> empty_2

    empty_3= EmptyOperator(
        task_id="empty_3"
    )
    empty_4= EmptyOperator(
        task_id="empty_4"
    )
    empty_5= EmptyOperator(
        task_id="empty_5"
    )
    
    empty_6= EmptyOperator(
        task_id="empty_6"
    )
    empty_2 >> Label('2-->3,4,5') >> [ empty_3, empty_4, empty_5] >> Label('3,4,5-->6') >> empty_6
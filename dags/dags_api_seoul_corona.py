from operators.ApiToCsvOperator_Seoul1 import ApiToCsvOperator_Seoul
from airflow import DAG
import pendulum


with DAG (
    dag_id="dags_api_seoul_corona",
    schedule="0 7 * * * ",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    tb_corona19_count_status=ApiToCsvOperator_Seoul(
        task_id="tb_corona19_count_status",
        dataset_nm="TbCorona19CountStatus",
        path="/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}",
        file_name="TbCorona19CountStatus_{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}.csv",
        apikey = "{{ var.value.apikey_openapi_seoul_go_kr | default('') }}"
    )

    tv_corona19_vaccine_status=ApiToCsvOperator_Seoul(
        task_id="tv_corona19_vaccine_status",
        dataset_nm="TvCorona19VaccineStatus",
        path="/opt/airflow/files/TvCorona19VaccineStatus/{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}",
        file_name="TvCorona19VaccineStatus_{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}.csv",
        apikey = "{{ var.value.apikey_openapi_seoul_go_kr | default('') }}"
    )


    tb_corona19_count_status >> tv_corona19_vaccine_status
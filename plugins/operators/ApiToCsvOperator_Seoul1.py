from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import os
import pandas as pd
import requests


class ApiToCsvOperator_Seoul(BaseOperator):
    template_fields = ['path', 'file_name']
    
    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = "openapi.seoul.go.kr.http"
        self.dataset_nm = dataset_nm
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{ var.value.apikey_openapi_seoul_go_kr }}/json/' + dataset_nm
        self.base_dt = base_dt
        self.base_url = f"http://{self.endpoint}"
        print(self.endpoint)

    def execute(self, context):
        import os

        self.log.info(f"self.endpoint:{self.endpoint}")
        self.log.info({{var.value.apikey_openapi_seoul_go_kr}})
        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f"http://{connection.host}:{connection.port}/{self.endpoint}"

        total_df = pd.DataFrame()
        start_row = 1
        add_row= 1000
        end_row = add_row

        while True:
            print(f"Fetching data: Start Row = {start_row}, End Row = {end_row}")
            
            try:
                start_row= start_row + add_row
                end_row = end_row + add_row 
                base_url = f"{self.base_url}/{start_row}/{end_row}"
                print(base_url)

                # 데이터 가져오기
                new_df = self._call_api(base_url, "json", "row", start_row, end_row)
                total_df = pd.concat([total_df, new_df], ignore_index=True)

                if len(new_df) <add_row :
                    print(len(new_df),"데이터 끝")
                    break

            except Exception as e:
                # print('raise_for_status', response.text)
                print(f"API 호출 실패: {e}")
                break

        # 파일 저장
        self._write_csv(total_df)

    # 실제 API 호출 함수 
    def _call_api(self, base_url, base_format, key_name, start_row, end_row):
        response = requests.get(base_url)
        if response.status_code != 200:
            raise Exception(f"API 호출 실패: {response.text}")
        
        try:
            if base_format=="json":
                data=response.json() 
                df = pd.DataFrame(data.get(self.dataset_nm).get(key_name)) # pd.json_normalize(data['LampScpgmtb']['row'])
        except Exception as e:
            print('raise_for_status', response.text)
            print(f"API 호출 실패: {e}")
        
        return df
    
    # 파일 저장
    def _write_csv(self, df):
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        
        if len(df):
            output_file = os.path.join(self.path, self.file_name)
            df.to_csv(output_file, index=False, encoding="utf-8-sig")
            print(f"파일 저장 완료: {output_file}") 


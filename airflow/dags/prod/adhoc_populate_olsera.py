from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.bash import BashOperator
from airflow.models.connection import Connection
import requests
import pyodbc
import pandas as pd
import traceback
from airflow.operators.dummy import DummyOperator
import json
from sqlalchemy.engine import URL, create_engine
from utils.utils import transformation_data, url
from utils.request import req_token

# Load configurations
with open('/opt/airflow/dags/creds/cred_outlet.json') as f:
    list_outlet = json.load(f).keys()
with open('/opt/airflow/dags/creds/cred_db.json') as f:
    db_con = json.load(f)

# Constants
mssql_conn_id = 'mssql_local'
start_date = '2023-09-09'
end_date = '2023-09-11'
connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={db_con["server"]};DATABASE={db_con["database"]};UID={db_con["username"]};PWD={db_con["password"]};Trusted_connection=no;TrustServerCertificate=yes;Encrypt=no;'
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})


def fetch_and_transform_data(token_type, access_token, outlet_name, table_name, url_path, detail=False, date_filter=True):
    header = {'Authorization': f'{token_type} {access_token}', 'Content-Type': 'application/json'}
    print(connection_url)
    print(f'access token : {access_token}')
    conn_engine = create_engine(connection_url)
    i = 1
    last_page = 1
    url_path = url + url_path 
    try:
        if detail:
            if date_filter:
                query = f"SELECT distinct id FROM dbo.{table_name.replace('_detail', '')} where CAST(order_date as date) >= '{start_date}' and CAST(order_date as date) <= '{end_date}' and outlet_name = '{outlet_name}';"
            else:
                query = f"SELECT distinct id FROM dbo.{table_name.replace('_detail', '')} where outlet_name = '{outlet_name}';"
            df = pd.read_sql(query, conn_engine)
            for i in df['id'].tolist():
                link = f'{url_path}?id={i}'
                response = requests.get(link, headers=header).json()
                transformation_data(table_name=table_name, datas=response['data'], details='yes', outlet_name=outlet_name, conn_engine=conn_engine)
        else:
            while i <= last_page:
                if date_filter:
                    link = f'{url_path}?per_page=100&page={i}&start_date={start_date}&end_date={end_date}'
                else:
                    link = f'{url_path}?per_page=100&page={i}'
                print(f'link : {link}')
                response = requests.get(link, headers=header).json()
                print(response)
                link = response['links']['next']
                last_page = response['meta']['last_page']
                transformation_data(table_name=table_name, datas=response['data'], details='no', outlet_name=outlet_name, conn_engine=conn_engine)
                i += 1
        return True
    except Exception:
        traceback.print_exc()
        return False

def close_order(token_type, access_token, outlet_name):
    return fetch_and_transform_data(token_type, access_token, outlet_name, table_name = 'close_order', url_path = f'/en/order/closeorder', detail=False, date_filter=True)

def close_order_detail(token_type, access_token, outlet_name):
    return fetch_and_transform_data(token_type, access_token, outlet_name, table_name = 'close_order_detail', url_path = f'/en/order/closeorder/detail', detail=True, date_filter=True)



# DAG definition

DEFAULT_TASK_ARGS = {
    "owner": "mangpendi",
    "retries": 5,
    "retry_delay": 60,
}

dag = DAG(
    dag_id='adhoc_populate_olsera',
    tags=['olsera'],
    catchup=False,
    template_searchpath=['/opt/airflow/dags/templates'],
    default_args=DEFAULT_TASK_ARGS,
)

start = DummyOperator(dag=dag, task_id='start_dag')
finish = DummyOperator(dag=dag, task_id='finish_dag')
rescan_field = BashOperator(dag=dag, task_id="rescan_field", bash_command="rescan_field_metabase")
rescan_schema = BashOperator(dag=dag, task_id="rescan_schema", bash_command="rescan_schema_metabase")

for outlet_name in list_outlet:
    token_type, access_token, _ = req_token(outlet_name)
    populate_close_order = ShortCircuitOperator(dag=dag, task_id=f'populate_close_order_{outlet_name}', python_callable=close_order, op_kwargs={'token_type': token_type, 'access_token': access_token, 'outlet_name': outlet_name})
    delete_close_order = MsSqlOperator(dag=dag, task_id=f'delete_close_order_{outlet_name}', mssql_conn_id=mssql_conn_id, autocommit = True, sql=f'EXEC dbo.sp_delete_close_order @start_date = \'{start_date}\', @end_date = \'{end_date}\', @outlet_name = \'{outlet_name}\'')
    populate_close_order_detail = ShortCircuitOperator(dag=dag, task_id=f'populate_close_order_detail_{outlet_name}', python_callable=close_order_detail, op_kwargs={'token_type': token_type, 'access_token': access_token, 'outlet_name': outlet_name})
    populate_close_order_detail_orderitems = MsSqlOperator(dag=dag, task_id=f'populate_close_order_detail_orderitems_{outlet_name}', mssql_conn_id=mssql_conn_id, sql=f'EXEC dbo.sp_insert_close_order_detail_orderitem @start_date = \'{start_date}\', @end_date = \'{end_date}\', @outlet_name = \'{outlet_name}\'')
    delete_close_order_detail = MsSqlOperator(dag=dag, task_id=f'delete_close_order_detail_{outlet_name}', mssql_conn_id=mssql_conn_id, autocommit = True, sql=f'EXEC dbo.sp_delete_close_order_detail @start_date = \'{start_date}\', @end_date = \'{end_date}\', @outlet_name = \'{outlet_name}\'')
    delete_close_order_detail_orderitem = MsSqlOperator(dag=dag, task_id=f'delete_close_order_detail_orderitem_{outlet_name}', mssql_conn_id=mssql_conn_id, autocommit = True, sql=f'EXEC dbo.sp_delete_close_order_detail_orderitem @start_date = \'{start_date}\', @end_date = \'{end_date}\', @outlet_name = \'{outlet_name}\'')

    start >> delete_close_order >> populate_close_order  >> delete_close_order_detail >> populate_close_order_detail  >> delete_close_order_detail_orderitem >> populate_close_order_detail_orderitems >> finish

finish >> rescan_field >> rescan_schema

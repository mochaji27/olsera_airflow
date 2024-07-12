from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.models.connection import Connection

import utils.request

tmpl_search_path = '/tmp/' 


with DAG(
    dag_id = 'populate_staff',
    schedule = None,
    start_date=datetime(2024, 3, 2),
    tags=['example'],
    catchup=False,
    template_searchpath = [tmpl_search_path]
) as dag:
    outlet_name = 'cijawa'
    token_type, access_token, refresh_token = utils.request.req_token(outlet_name)
    request_staff = PythonOperator(task_id = 'request_staff', python_callable = utils.request.req_staff, op_kwargs = {'token_type' : token_type, 'access_token' : access_token, 'outlet_name' : outlet_name})
    populate_staff_table = MsSqlOperator(task_id = 'populate_staff_table', mssql_conn_id = 'mssql_local', sql = 'sqlserver_query_staff.sql')
    
    request_staff >> populate_staff_table




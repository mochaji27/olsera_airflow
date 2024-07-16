from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.bash_operator import BashOperator
import requests
from datetime import datetime
from utils.utils import *
import pyodbc
import pandas as pd
import traceback
from utils.request import req_token
from airflow.operators.dummy import DummyOperator
import json

# Load configurations
with open('/opt/airflow/dags/creds/cred_outlet.json') as f:
    list_outlet = json.load(f).keys()
with open('/opt/airflow/dags/creds/cred_db.json') as f:
    db_con = json.load(f)

# Constants
mssql_conn_id = 'mssql_local'
connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={db_con["server"]};DATABASE={db_con["database"]};UID={db_con["username"]};PWD={db_con["password"]};Trusted_connection=no;TrustServerCertificate=yes;Encrypt=no;'
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

def fetch_and_transform_data(token_type, access_token, outlet_name, table_name, url_path, detail=False, date_filter=True):
    header = {'Authorization': f'{token_type} {access_token}', 'Content-Type': 'application/json'}
    print(connection_url)
    print(f'access token : {access_token}')
    conn_engine = create_engine(connection_url)
    url_path = url + url_path 
    date_now = get_date_now()
    try:
        if detail:
            if date_filter:
                query = f"SELECT distinct id FROM dbo.{table_name.replace('_detail', '')} where CAST(order_date as date) >= '{date_now}' and CAST(order_date as date) <= '{date_now}' and outlet_name = '{outlet_name}';"
            else:
                query = f"SELECT distinct id FROM dbo.{table_name.replace('_detail', '')} where outlet_name = '{outlet_name}';"
            df = pd.read_sql(query, conn_engine)
            for i in df['id'].tolist():
                link = f'{url_path}?id={i}'
                response = requests.get(link, headers=header).json()
                transformation_data(table_name=table_name, datas=response['data'], details='yes', outlet_name=outlet_name, conn_engine=conn_engine)
        else:
            last_page = 1
            i = 1
            while i <= last_page:
                if date_filter:
                    link = f'{url_path}?per_page=100&page={i}&start_date={date_now}&end_date={date_now}'
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

def products(token_type, access_token, outlet_name):
    return fetch_and_transform_data(token_type, access_token, outlet_name, table_name = 'products', url_path = f'/en/product', detail=False, date_filter=False)

def products_detail(token_type, access_token, outlet_name):
    return fetch_and_transform_data(token_type, access_token, outlet_name, table_name = 'products_detail', url_path = f'/en/product/detail', detail=True, date_filter=False)

def products_combo(token_type, access_token, outlet_name):
    return fetch_and_transform_data(token_type, access_token, outlet_name, table_name = 'products_combo', url_path = f'/en/productcombo', detail=False, date_filter=False)

def products_combo_detail(token_type, access_token, outlet_name):
    return fetch_and_transform_data(token_type, access_token, outlet_name, table_name = 'products_combo_detail', url_path = f'/en/productcombo/detail', detail=True, date_filter=False)

def products_group(token_type, access_token, outlet_name):
    return fetch_and_transform_data(token_type, access_token, outlet_name, table_name = 'products_group', url_path = f'/en/productgroup', detail=False, date_filter=False)


list_table_product = ["products_group", "products", "products_combo"]
list_table_product_detail = ["products_detail", "products_combo_detail"]
list_table_product_detail_json = ["products_combo_detail_items"]

list_table_order = ["close_order"]
list_table_order_detail = ["close_order_detail"]
list_table_order_detail_json = ["close_order_detail_orderitem"]

list_function = {
    "products_group" : products_group,
    "products" : products,
    "products_detail" : products_detail,
    "products_combo" : products_combo,
    "products_combo_detail" : products_combo_detail,
    "close_order" : close_order,
    "close_order_detail" : close_order_detail,
}

DEFAULT_TASK_ARGS = {
    "owner": "mangpendi",
    "retries": 3,
    "retry_delay": 120,
}
dag = DAG(
    dag_id = f'populate_olsera',
    schedule_interval='0 15 * * *',
    start_date=datetime(2024, 6, 10),
    tags=['olsera'],
    catchup=False,
    template_searchpath = [tmpl_search_path],
    default_args=DEFAULT_TASK_ARGS,
)

def check_skip(dag, name = ""):
    return DummyOperator(dag = dag, task_id = f'check_skip_{name}', trigger_rule = 'none_failed')

start = DummyOperator(dag = dag, task_id = 'start_dag')
start_order = DummyOperator(dag = dag, task_id = 'start_order', trigger_rule = 'none_failed')
finish_all_order = DummyOperator(dag = dag, task_id = 'finish_all_order')
finish_close_order = DummyOperator(dag = dag, task_id = 'finish_close_order')
finish_close_order_detail = DummyOperator(dag = dag, task_id = 'finish_close_order_detail')
finish_close_order_detail_json = DummyOperator(dag = dag, task_id = 'finish_close_order_detail_json')
start_products = DummyOperator(dag = dag, task_id = 'start_products')
finish_products = DummyOperator(dag = dag, task_id = 'finish_products', trigger_rule = 'none_failed')
finish_all_products = DummyOperator(dag = dag, task_id = 'finish_all_products', trigger_rule = 'none_failed')
finish_products_detail = DummyOperator(dag = dag, task_id = 'finish_products_detail', trigger_rule = 'none_failed')
finish_products_detail_json = DummyOperator(dag = dag, task_id = 'finish_products_detail_json', trigger_rule = 'none_failed')
finish = DummyOperator(dag = dag, task_id = 'finish_dag')

rescan_field = BashOperator(task_id="rescan_field", bash_command=rescan_field_metabase)
rescan_schema = BashOperator(task_id="rescan_schema", bash_command=rescan_schema_metabase)
    

for outlet_name in list_outlet:
    token_type, access_token, refresh_token = req_token(outlet_name)
    
    for table in list_table_product:
        delete_product = MsSqlOperator(dag = dag, task_id = f'delete_{table}_{outlet_name}', mssql_conn_id = mssql_conn_id, autocommit = True, sql = f'DELETE FROM dbo.{table} WHERE outlet_name = \'{outlet_name}\';')
        populate_product = ShortCircuitOperator(dag = dag, task_id = f'populate_{table}_{outlet_name}', python_callable = list_function[table], op_kwargs = {'token_type' : token_type, 'access_token' : access_token, 'outlet_name' : outlet_name}, ignore_downstream_trigger_rules=False)
        check = check_skip(dag, f'{table}_{outlet_name}')
        start >> start_products >> delete_product >> populate_product >> check >> finish_products
    
    for table in list_table_product_detail:
        delete_product_detail = MsSqlOperator(dag = dag, task_id = f'delete_{table}_{outlet_name}', mssql_conn_id = mssql_conn_id, autocommit = True, sql = f'EXEC dbo.sp_delete_{table} @outlet_name = \'{outlet_name}\';')
        populate_product_detail = ShortCircuitOperator(dag = dag, task_id = f'populate_{table}_{outlet_name}', python_callable = list_function[table], op_kwargs = {'token_type' : token_type, 'access_token' : access_token, 'outlet_name' : outlet_name}, ignore_downstream_trigger_rules=False)
        check = check_skip(dag, f'{table}_{outlet_name}')
        finish_products >> delete_product_detail >> populate_product_detail >> check >> finish_products_detail

    for table in list_table_product_detail_json:
        delete_product_detail_json = MsSqlOperator(dag = dag, task_id = f'delete_{table}_{outlet_name}', mssql_conn_id = 'mssql_local', autocommit = True, sql = f'EXEC dbo.sp_delete_{table} @outlet_name = \'{outlet_name}\';')
        populate_product_detail_json = MsSqlOperator(dag = dag, task_id = f'populate_{table}_{outlet_name}', mssql_conn_id = 'mssql_local', sql = f'EXEC dbo.sp_insert_{table} @outlet_name = \'{outlet_name}\';')
        check = check_skip(dag, f'{table}_{outlet_name}')
        finish_products_detail >> delete_product_detail_json >> populate_product_detail_json >> finish_products_detail_json >> check >> finish_all_products

    for table in list_table_order:
        delete_order = MsSqlOperator(dag = dag, task_id = f'delete_{table}_{outlet_name}', mssql_conn_id = mssql_conn_id, autocommit = True, sql = f'EXEC dbo.sp_delete_{table} @start_date = \'{get_date_now()}\', @end_date = \'{get_date_now()}\', @outlet_name = \'{outlet_name}\';')
        populate_order = PythonOperator(dag = dag, task_id = f'populate_{table}_{outlet_name}', python_callable = list_function[table], op_kwargs = {'token_type' : token_type, 'access_token' : access_token, 'outlet_name' : outlet_name})
    
        finish_all_products >> start_order >> delete_order >> populate_order >> finish_close_order

    for table in list_table_order_detail:
        delete_order_detail = MsSqlOperator(dag = dag, task_id = f'delete_{table}_{outlet_name}', mssql_conn_id = mssql_conn_id, autocommit = True, sql = f'EXEC dbo.sp_delete_{table} @start_date = \'{get_date_now()}\', @end_date = \'{get_date_now()}\', @outlet_name = \'{outlet_name}\';')
        populate_order_detail = PythonOperator(dag = dag, task_id = f'populate_{table}_{outlet_name}', python_callable = list_function[table], op_kwargs = {'token_type' : token_type, 'access_token' : access_token, 'outlet_name' : outlet_name})

        finish_close_order >> delete_order_detail >> populate_order_detail >> finish_close_order_detail

    for table in list_table_order_detail_json:
        delete_order_detail_json = MsSqlOperator(dag = dag, task_id = f'delete_{table}_{outlet_name}', mssql_conn_id = 'mssql_local', autocommit = True, sql = f'EXEC dbo.sp_delete_{table} @start_date = \'{get_date_now()}\', @end_date = \'{get_date_now()}\', @outlet_name = \'{outlet_name}\';')
        populate_order_detail_json = MsSqlOperator(dag = dag, task_id = f'populate_{table}_{outlet_name}', mssql_conn_id = 'mssql_local', sql = f'EXEC dbo.sp_insert_{table} @start_date = \'{get_date_now()}\', @end_date = \'{get_date_now()}\', @outlet_name = \'{outlet_name}\';')

        finish_close_order_detail >> delete_order_detail_json >> populate_order_detail_json >> finish_close_order_detail_json >> finish_all_order

finish_all_order >> rescan_field >> rescan_schema >> finish
    

# for outlet_name in list_outlet:
#     token_type, access_token, refresh_token = req_token(outlet_name)

#     delete_products_group = MsSqlOperator(dag = dag, task_id = f'delete_products_group_{outlet_name}', mssql_conn_id = mssql_conn_id, autocommit = True, sql = f'DELETE FROM dbo.products_group WHERE outlet_name = \'{outlet_name}\';')
#     populate_products_group = ShortCircuitOperator(dag = dag, task_id = f'populate_products_group_{outlet_name}', python_callable = products_group, op_kwargs = {'token_type' : token_type, 'access_token' : access_token, 'outlet_name' : outlet_name}, ignore_downstream_trigger_rules=False)

#     delete_products = MsSqlOperator(dag = dag, task_id = f'delete_products_{outlet_name}', mssql_conn_id = mssql_conn_id, autocommit = True, sql = f'DELETE FROM dbo.products WHERE outlet_name = \'{outlet_name}\';')
#     populate_products = ShortCircuitOperator(dag = dag, task_id = f'populate_products_{outlet_name}', python_callable = products, op_kwargs = {'token_type' : token_type, 'access_token' : access_token, 'outlet_name' : outlet_name}, ignore_downstream_trigger_rules=False)
#     delete_products_detail = MsSqlOperator(dag = dag, task_id = f'delete_products_detail_{outlet_name}', mssql_conn_id = mssql_conn_id, autocommit = True, sql = f'DELETE dbo.products_detail from dbo.products_detail inner join products on products_detail.id = products.id  WHERE outlet_name = \'{outlet_name}\';')
#     populate_products_detail = ShortCircuitOperator(dag = dag, task_id = f'populate_products_detail_{outlet_name}', python_callable = products_detail, op_kwargs = {'token_type' : token_type, 'access_token' : access_token, 'outlet_name' : outlet_name}, ignore_downstream_trigger_rules=False)
    
#     delete_products_combo = MsSqlOperator(dag = dag, task_id = f'delete_products_combo_{outlet_name}', mssql_conn_id = mssql_conn_id, autocommit = True, sql = f'DELETE FROM dbo.products_combo WHERE outlet_name = \'{outlet_name}\';')
#     populate_products_combo = ShortCircuitOperator(dag = dag, task_id = f'populate_products_combo_{outlet_name}', python_callable = products_combo, op_kwargs = {'token_type' : token_type, 'access_token' : access_token, 'outlet_name' : outlet_name}, ignore_downstream_trigger_rules=False)
    
#     delete_products_combo_detail = MsSqlOperator(dag = dag, task_id = f'delete_products_combo_detail_{outlet_name}', mssql_conn_id = mssql_conn_id, autocommit = True, sql = f'DELETE dbo.products_combo_detail from dbo.products_combo_detail inner join products_combo on products_combo_detail.id = products_combo.id  WHERE outlet_name = \'{outlet_name}\';')
#     populate_products_combo_detail = ShortCircuitOperator(dag = dag, task_id = f'populate_products_combo_detail_{outlet_name}', python_callable = products_combo_detail, op_kwargs = {'token_type' : token_type, 'access_token' : access_token, 'outlet_name' : outlet_name}, ignore_downstream_trigger_rules=False)
    
#     delete_products_combo_detail_items = MsSqlOperator(dag = dag, task_id = f'delete_products_combo_detail_items_{outlet_name}', mssql_conn_id = 'mssql_local', autocommit = True, sql = f'EXEC dbo.sp_delete_products_combo_detail_items @outlet_name = \'{outlet_name}\';')
#     populate_products_combo_detail_items = MsSqlOperator(dag = dag, task_id = f'populate_products_combo_detail_items_{outlet_name}', mssql_conn_id = 'mssql_local', sql = f'EXEC dbo.sp_insert_products_combo_detail_items @outlet_name = \'{outlet_name}\';')

#     start >> start_products  >> delete_products >> populate_products >> delete_products_detail >> populate_products_detail >> finish_products
#     finish_products >> delete_products_combo >> populate_products_combo >> delete_products_combo_detail >> populate_products_combo_detail >> delete_products_combo_detail_items >> populate_products_combo_detail_items >> finish_products_combo

    
   
#     populate_close_order = ShortCircuitOperator(dag = dag, task_id = f'populate_close_order_{outlet_name}', python_callable = close_order, op_kwargs = {'token_type' : token_type, 'access_token' : access_token, 'outlet_name' : outlet_name}, ignore_downstream_trigger_rules=False)
#     delete_close_order = MsSqlOperator(dag = dag, task_id = f'delete_close_order_{outlet_name}', mssql_conn_id = mssql_conn_id, autocommit = True, sql = f'EXEC dbo.sp_delete_close_order @start_date = \'{get_date_now()}\', @end_date = \'{get_date_now()}\', @outlet_name = \'{outlet_name}\';')
#     populate_close_order_detail = ShortCircuitOperator(dag = dag, task_id = f'populate_close_order_detail_{outlet_name}', python_callable = close_order_detail, op_kwargs = {'token_type' : token_type, 'access_token' : access_token, 'outlet_name' : outlet_name}, ignore_downstream_trigger_rules=False)
#     populate_close_order_detail_orderitems = MsSqlOperator(dag = dag, task_id = f'populate_close_order_detail_orderitems_{outlet_name}', mssql_conn_id = mssql_conn_id, autocommit = True, sql = f'EXEC dbo.sp_insert_close_order_detail_orderitem @start_date = \'{get_date_now()}\', @end_date = \'{get_date_now()}\', @outlet_name = \'{outlet_name}\';')
#     delete_close_order_detail = MsSqlOperator(dag = dag, task_id = f'delete_close_order_detail_{outlet_name}', mssql_conn_id = mssql_conn_id, autocommit = True ,sql = f'EXEC dbo.sp_delete_close_order_detail @start_date = \'{get_date_now()}\', @end_date = \'{get_date_now()}\', @outlet_name = \'{outlet_name}\';')
#     delete_close_order_detail_orderitem = MsSqlOperator(dag = dag, task_id = f'delete_close_order_detail_orderitem_{outlet_name}', mssql_conn_id = mssql_conn_id, autocommit = True, sql = f'EXEC dbo.sp_delete_close_order_detail_orderitem @start_date = \'{get_date_now()}\', @end_date = \'{get_date_now()}\', @outlet_name = \'{outlet_name}\';')


#     finish_products_combo >> start_close_order >> delete_close_order >> populate_close_order  >> delete_close_order_detail >> populate_close_order_detail >> delete_close_order_detail_orderitem >> populate_close_order_detail_orderitems >> finish_close_order >> finish


# finish >> rescan_field >> rescan_schema
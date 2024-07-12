from datetime import datetime, timedelta
import os
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import pandas as pd


tmpl_search_path = '/tmp/' 
url = 'https://api-open.olsera.co.id/api/open-api/v1'
file_path = '/tmp/sqlserver_query_xxx.sql'

rescan_field_metabase = "curl -H 'x-api-key: mb_Cpv4J/7TzcWjEaUQYMNBVZmOmVrC7uw5Sw5Gpz6mpKQ=' -X POST 'http://192.168.1.16:3000/api/database/3/rescan_values'"
rescan_schema_metabase = "curl -H 'x-api-key: mb_Cpv4J/7TzcWjEaUQYMNBVZmOmVrC7uw5Sw5Gpz6mpKQ=' -X GET 'http://192.168.1.16:3000/api/database/3/syncable_schemas'"


def get_date_now():
    return (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%d')


def replace_value_json(value):
    value_str = str(value)
    if 'IDR' in value_str:
        value_str = value_str.replace('.', '').replace('IDR ', '')
    if '{' in value_str:
        value_str = value_str.replace('"', '').replace('\'', '"')
    else:
        value_str = value_str.replace('.00', '')
    value_str = value_str.replace('0000-00-00 00:00:00', '1999-01-01 12:00:00')
    if value_str in {"None", "''"}:
        return None
    return value_str

def transformation_data(table_name, datas, details = 'no', outlet_name='', conn_engine = ''):
    current_time = get_date_now()
    if details == 'no': 
        for data in datas:
            data.update({key: replace_value_json(value) for key, value in data.items()})
            data['timestamp'] = current_time
            data['outlet_name'] = outlet_name
    elif details == 'yes': 
        datas.update({key: replace_value_json(value) for key, value in datas.items()})
        datas['timestamp'] = current_time
    df = pd.json_normalize(datas)
    df.to_sql(name = table_name, con = conn_engine, index = False, if_exists='append')
    #print(f'Inserted {df.shape[0]} row')
    

import requests
from datetime import datetime, timedelta
from utils.utils import *
import json


url = 'https://api-open.olsera.co.id/api/open-api/v1'
file_path = '/tmp/sqlserver_query_xxx.sql'

def req_token(outlet_name):
    list_outlet = json.load(open('/opt/airflow/dags/creds/cred_outlet.json'))
    header = {'Content-Type' : 'application/json'}
    body = {'app_id' : list_outlet[outlet_name]['app_id']
            ,'secret_key' : list_outlet[outlet_name]['secret_key']
            ,'grant_type' : 'secret_key'}
    req = requests.post(url + '/id/token', headers=header, json=body).json()
    return req['token_type'], req['access_token'], req['refresh_token']
    

def req_refresh_token(refresh_token):
    header = {'Content-Type' : 'application/json'}
    body = {'refresh_token' : refresh_token
            ,'grant_type' : 'refresh_token'}
    req = requests.post(url + '/id/token', headers=header, json=body).json()
    return req['token_type'], req['access_token'], req['refresh_token']


 
 


def req_staff(token_type, access_token):
    header = {'Authorization': token_type + ' ' + access_token
            ,'Content-Type' : 'application/json'}
    last_page = 1
    i = 1
    while (i <= last_page):
        link = url + f'/en/global/storeusers?per_page=15&page={i}&start_date=2023-01-01&end_date=2024-01-01'
        req = requests.get(link, headers=header).json()
        link = req['links']['next']
        last_page = req['meta']['last_page']
        print('keys : ' + str(req.keys()))
        print('length data : ' + str(len(req['data'])))
        print(req['data'])
        print(req['status'])
        i += 1

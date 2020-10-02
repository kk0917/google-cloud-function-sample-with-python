import json
import os

from lib.sql_connection import insert, select
from google.cloud import bigquery

DATASET_ID = os.environ['BIGQUERY_DATASET_ID']
TABLE_ID   = os.environ['BIGQUERY_TABLE_ID']

def main(request):
    req_params = setRequestParams(request)
    resp       = getMasterData(req_params)

    if resp != None:
        return convert_data2json('master_db', resp)
    else:
        result  = identify_company_name(req_params['target_name'])
        to_json = ''

        # ...

        return convert_data2json('bigquery', to_json)

def setRequestParams(request):
    return {
        "sys_id": request.args.get('sys_id'),
        "sys_master_id": request.args.get('sys_master_id'),
        "target_name": request.args.get('target_name')
    }

def getMasterData(req_params):
    return select(req_params)

def convert_data2json(reference, resp):
    dicts = {}

    if reference == 'master_db':
        for i, row in enumerate(resp):
            _dict = {
                "id":          row.id,
                "unique_name": row.unique_name
            }

            dicts.setdefault(i, _dict)
    elif reference == 'bigquery':
        print('aaa') # TODO:
    else:
        dicts = {"message": "Unknown reference"}

    return json.dumps(dicts, indent=4)

def identify_company_name(target_name):
    fmt_name_str = fmt_string(target_name)
    return eval_company_name(fmt_name_str)

def fmt_string(target_name): # TODO: fix inappropriate variables name
    _fmt_name_str = target_name

    # ...

    return _fmt_name_str

def eval_company_name(fmt_name_str):
    company_names_dic   = get_company_names_dic()
    name_list = company_names_dic

    # ...

    return name_list

def get_company_names_dic():
    client    = bigquery.Client()
    query_job = client.query('SELECT string_field_0 FROM `dac-techdev0:jcl_dic.jcl_dic`')

    results = query_job.result()  # Waits for job to complete.

    for row in results:
        print("{} : {} views".fmt(row.url, row.view_count))
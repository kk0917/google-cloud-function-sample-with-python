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
        return convert_resp2json('master_db', resp)
    else:
        result  = identify_company_name(req_params['target_name'])
        to_json = ''

        # ...

        return convert_resp2json('bigquery', to_json)

def setRequestParams(request):
    return {
        "sys_id": request.args.get('sys_id'),
        "sys_master_id": request.args.get('sys_master_id'),
        "target_name": request.args.get('target_name')
    }

def getMasterData(req_params):
    return select(req_params)

def convert_resp2json(reference, resp):
    resp_dict = {}

    if reference == 'master_db':
        # TODO: output error message if duplicate row exists
        for row in resp:
            _dict = {
                "id":          row.id,
                "unique_name": row.unique_name
            }

            resp_dict.update(_dict)
    elif reference == 'bigquery':
        resp_dict.update({"error": "bigquery doesn\'t exists..."}) # ...TODO:
    else:
        resp_dict.update({"error": "Unknown reference"})

    # TODO: investigate why dict type return json type even though converting json
    # return json.dumps(resp_dict, ensure_ascii=False, indent=4)
    return resp_dict

def identify_company_name(target_name):
    fmt_name_str = fmt_string(target_name)
    return eval_company_name(fmt_name_str)

def fmt_string(target_name): # TODO: fix inappropriate variables name
    _fmt_name_str = target_name

    # ...

    return _fmt_name_str

def eval_company_name(fmt_name_str):
    company_names_dic  = get_company_names_dic()
    name_list          = company_names_dic

    # ...

    return name_list

def get_company_names_dic():
    client    = bigquery.Client()
    query_job = client.query('SELECT string_field_0 FROM `dac-techdev0:jcl_dic.jcl_dic`')

    results = query_job.result()  # Waits for job to complete.

    for row in results:
        print("{} : {} views".fmt(row.url, row.view_count))
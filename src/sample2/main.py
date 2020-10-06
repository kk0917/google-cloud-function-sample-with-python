import json
import os
import re
import unicodedata

from lib.db_connection import insert, select
from google.cloud import bigquery

DATASET_ID = os.environ['BIGQUERY_DATASET_ID']
TABLE_ID   = os.environ['BIGQUERY_TABLE_ID']

def main(request):
    req_params = setRequestParams(request)
    result     = fetchMasterData(req_params) # ResultProxy obj
    resp       = result.fetchall()           # list obj

    if 2 > len(resp) > 0:
        return convert_resp2json('master_db', resp)
    # elif len(resp) > 1:
    #     return json.dumps({"error": "duplicate data Exists..."}, indent=4)
    else:
        bq_result = get_company_names_dic(req_params['target_name'])
        for row in bq_result:
            return str(row)
        # return identify_company_name(req_params['target_name'])

        to_json = ''

        if len(result) == 1: # TODO: [and len(result) > 1:]?
            to_json = result
            insert(req_params.sys_id, req_params.sys_master_id, result.unique_name) # TODO: update
        elif len(result) > 1: # TODO: enable marge with if?
            to_json = result
        else:
            to_json = {"message": "couldn't find..."}
        
        return convert_resp2json('_bigquery', to_json)

def setRequestParams(request):
    return {
        "sys_id": request.args.get('sys_id'),
        "sys_master_id": request.args.get('sys_master_id'),
        "target_name": request.args.get('target_name')
    }

def fetchMasterData(req_params):
    return select(req_params)

def convert_resp2json(reference, resp):
    resp_dict = {}

    if reference == 'master_db':
        # TODO: output error message if duplicate row exists
        for row in resp:
            _dict = {
                "id":          row['id'],
                "unique_name": row['unique_name']
            }

            resp_dict.update(_dict)
    elif reference == '_bigquery':
        for row in resp:
            _dict = {
                "string_field_0": row.string_field_0
            }

            resp_dict.update(_dict)
    else:
        resp_dict.update({"error": "Unknown reference"})

    # return insert(request) # TODO: update after completing inprementation

def identify_company_name(target_name):
    fmt_name_str = fmt_string(target_name)
    return eval_company_name(fmt_name_str)

def fmt_string(target_name): # TODO: fix inappropriate variables name
    _fmt_name_str = 'aaa'

    return _fmt_name_str

def eval_company_name(fmt_name_str):
    fetchOne = ''

    # ...

    return fetchOne

def get_company_names_dic(target_name):
    client    = bigquery.Client()
    # query_job = client.query("SELECT string_field_0 FROM `dac-techdev0.jcl_dic.jcl_dic` WHERE string_field_0 = {}".format(target_name))
    query_job = client.query('SELECT string_field_0 FROM `dac-techdev0.jcl_dic.jcl_dic`')
    result    = query_job.result()  # Waits for job to complete.

    return result

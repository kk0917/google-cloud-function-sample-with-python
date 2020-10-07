import json
import os
import re
import unicodedata

from lib.db_connection import insert, select
from google.cloud import bigquery

DATASET_ID = os.environ['BIGQUERY_DATASET_ID']
TABLE_ID   = os.environ['BIGQUERY_TABLE_ID']

def main(request):
    req_params = set_request_params(request)
    resp       = fetch_master_data(req_params)

    if len(resp) == 1:
        result = convert_resp2json('master_db', resp)
    elif len(resp) > 1:
        result = convert_resp2json('duplicated_names', resp)
    else:
        identified_names = identify_company_name(req_params['target_name'])
        required_values = extract_required_values(identified_names, req_params)

        result = convert_resp2json('_bigquery', required_values)

    return result

def set_request_params(request):
    return {
        "sys_id": request.args.get('sys_id'),
        "sys_master_id": request.args.get('sys_master_id'),
        "target_name": request.args.get('target_name')
    }

def fetch_master_data(req_params):
    result = select(req_params)

    return result.fetchall() # convert SQLAlchemy.ResultProxy obj to list obj

def convert_resp2json(reference, resp):
    row_status = None
    resp_dict  = {}

    if reference == 'master_db':
        # TODO: output error message if duplicate row exists
        for row in resp:
            row_status = verify_row_status(row)

            _dict = {
                "id":          row['id'],
                "unique_name": row['unique_name'],
                "status":      row_status
            }

            resp_dict.update(_dict)
    elif reference == 'duplicated_names':
        for i, row in enumerate(resp):
            row_status = verify_row_status(row)

            _dict = {
                i: {
                    "id":          row['id'],
                    "unique_name": row['unique_name'],
                    "status":      row_status
                }
            }

            resp_dict.update(_dict)
    elif reference == '_bigquery':
        for i, row in enumerate(resp):
            _dict = {
                i: {
                    "string_field_0": row.string_field_0
                }
            }

            resp_dict.update(_dict)
    else:
        resp_dict.update({"error": "Unknown reference"})

    return resp_dict

def verify_row_status(_row):
    status = {
        0: "initial regist",
        1: "registed",
        2: "updated"
    }

    result = None

    if _row['created_at'] == _row['updated_at']:
        result = status[1]
    elif _row['created_at'] < _row['updated_at']:
        result = status[2]

    return result

def identify_company_name(target_name):
    fmt_name_str = fmt_string(target_name)
    return eval_company_name(fmt_name_str)

def extract_required_values(_identified_names, _req_params):
    total_rows = _identified_names.total_rows
    to_json = ''

    if total_rows == 1:
        to_json = _identified_names

        insert(_req_params['sys_id'], _req_params['sys_master_id'], _identified_names['unique_name'])
    elif total_rows > 1:
        to_json = _identified_names
    else:
        to_json = {"message": "couldn't find..."}
    
    return to_json

def fmt_string(target_name): # TODO: fix inappropriate variables name
    _fmt_name_str = 'aaa'

    return _fmt_name_str

def eval_company_name(fmt_name_str):
    fetchOne = ''

    if result == '': result = None
    """aaa"""

    return fetchOne

    """ TODO: Add registration status
        ex. {
            ...,
            "status": {
                0: "initial regist",
                1: "already registed",
                2: "updated after last time"
            }
        }
    """

def get_company_names_dic(target_name):
    client    = bigquery.Client()
    # query_job = client.query("SELECT string_field_0 FROM `dac-techdev0.jcl_dic.jcl_dic` WHERE string_field_0 = {}".format(target_name))
    query_job = client.query('SELECT string_field_0 FROM `dac-techdev0.jcl_dic.jcl_dic`')
    result    = query_job.result()  # Waits for job to complete.

    return result

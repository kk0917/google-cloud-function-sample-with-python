import re
import unicodedata

from lib.db_connection import insert, select, fetch_company_name_dic_bq

def main(request):
    req_params = set_request_params(request)
    resp       = fetch_master_data(req_params)
    result     = {"no matched": "names couldn't find..."}

    if len(resp) == 1:
        result = convert_resp2json('master_db', resp)
    elif len(resp) > 1:
        result = convert_resp2json('duplicated_names', resp)
    else:
        identified_names = identify_company_name(req_params['target_name'])
        result          = convert_resp2json('_bigquery', identified_names)

        if len(identified_names) == 1:
            insert(req_params, identified_names[0])

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
        for row in resp:
            row_status = verify_row_status(row)

            _dict = {
                "id":          row['id'],
                "unique_name": row['unique_name'],
                "status":      row_status
            }

            resp_dict.update(_dict)
    elif reference == 'duplicated_names':
        resp_dict.update({"error": 'Duplicated master datas...'})

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
        resp_dict.update(resp)

    else:
        resp_dict.update({"error": "Unknown reference"})

    return resp_dict # unnecessary converting to json...

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
    bq_result    = fetch_company_name_dic_bq(fmt_name_str)
    result       = {}

    if bq_result.total_rows == 1:
        for i, row in enumerate(bq_result):
            result = {i: row.get('string_field_0')}

    elif bq_result.total_rows > 1:
        rows = {}
        for i, row in enumerate(bq_result):
            rows.update({i: row.get('string_field_0')})

        result = rows

    return result

def fmt_string(target_name):
    _fmt_name_str = target_name

    # ...

    return _fmt_name_str

def delete_municipalities_str(target_name_str): # municipalities = all of cities, wards, towns and villages
    municipalities_str = ['aaa', 'bbb', 'ccc', 'ddd']

    # ...

    return target_name_str

def delete_brackets(target_name):
    # ...

    return target_name

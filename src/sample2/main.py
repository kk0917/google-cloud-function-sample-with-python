import re
import unicodedata

from flask import Response

from lib.db_connection import insert, select, fetch_company_name_dic_bq

def main(request):
    # TODO: add Request Error -> JSON Type? all parameters?
    content_type = request.headers['content-type']

    if content_type == 'application/json': # TODO: If you change the HTTP method to POST you need to update the headers and parameters values
        req_params = set_request_params(request)

        if 'sys_id' in request.args and 'sys_master_id' in request.args and 'target_name' in request.args:
            resp = fetch_master_data(req_params)
        else:
            return Response("Bad Request: JSON is invalid, or missing 'sys_id', 'sys_master_id', 'target_name' properties", 400)
    else:
        return Response("Bad Request: Unknown content type: {}".format(content_type), 400)

    if len(resp) == 1:
        result = generate_json_resp('master_db', resp)
    elif len(resp) > 1:
        result = generate_json_resp('duplicated_names', resp)
    else:
        identified_names = identify_company_name(req_params['target_name'])
        result           = generate_json_resp('_bigquery', identified_names)

        # if len(identified_names) == 1:
        #     return insert(req_params, identified_names[0])

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

def generate_json_resp(reference, resp):
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

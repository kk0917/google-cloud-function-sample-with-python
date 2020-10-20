import re
import unicodedata

from flask import Response

from lib.db_connection import insert, select
from lib.bq_connection import fetch_company_name_dic_bq

def main(request):
    """ Execution function specified by Cloud Functions

    Arg:
        request (Request): Http request information. headers, body, etc.
    Return:
        dict: Return result of fetched unique name from database, or identified name from JCL dictionary.
    """
    content_type = request.headers['content-type']

    if content_type == 'application/json':
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
        return Response("Server Error.., duplicate row. Please contact techDev0", 500)
    else:
        identified_names = identify_company_name(req_params['target_name'])
        result           = generate_json_resp('_bigquery', identified_names)

        if len(identified_names) == 1:
            insert(req_params, identified_names[0])
        elif len(identified_names)== 0:
            result = {}

    return result

def set_request_params(request):
    """ Sets request parameters to format

    Arg:
        request (Request): Http request information.

    Return:
        dict: Return request parameters into formats.
    """
    return {
        "sys_id": request.args.get('sys_id'),
        "sys_master_id": request.args.get('sys_master_id'),
        "target_name": request.args.get('target_name')
    }

def fetch_master_data(req_params):
    """ Fetch official unique company name from database

    Arg:
        req_params (dict): Http request parameters using search (database or BigQuery) or identify name string.

    return:
        list: fetch result. converted SQLAlchemy.ResultProxy obj to list obj
    """
    result = select(req_params)

    return result.fetchall() # convert SQLAlchemy.ResultProxy obj to list obj

def generate_json_resp(reference, resp):
    """ Generate HTTP Response body

    Args:
        reference (str): reference type, database or BigQuery.
        resp (): Fetched result of database or identified unique name
    return:
        dict: Http response json type body
    """
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

    elif reference == '_bigquery':
        resp_dict.update(resp)

    else:
        resp_dict.update({"error": "Unknown reference"})

    return resp_dict # unnecessary converting to json...

def verify_row_status(_row):
    """ verify fetched unique name from database.n

    Arg:
        _row (list): fetched data from database.
    Rreturn:
        str: status string.
    """
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
    """ Identify company name by JCL dictionary

    Arg:
        target_name (str): target name
    Return:
        dict: Identified unique name or candidate names from JCL dictionary into BigQuery
    """
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

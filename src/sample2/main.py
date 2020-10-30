import json
import re
import unicodedata

from flask import Response, make_response

from lib.db_connection import insert, select
from lib.bq_connection import fetch_company_name_dic_bq

def main(request):
    """ Execution function specified by Cloud Functions

    Arg:
        request (Request): Http request information. headers, body, etc.
    Return:
        dict: Return result of fetched unique name from database, or identified name from JCL dictionary.
    """
    try:
        content_type = request.headers['content-type']

        if not content_type == 'application/json':
            return Response("Bad Request: Unknown content type: {}".format(content_type), 400)

        if not 'sys_id' in request.args and not 'sys_master_id' in request.args and not 'target_name' in request.args:
            return Response("Bad Request: JSON is invalid, or missing 'sys_id', 'sys_master_id', 'target_name' properties", 400)

        req_params = set_request_params(request)
        bq_resp    = identify_company_name(req_params['target_name'])
        resp_body  = {}

        if len(bq_resp) == 1:
            resp_body = get_response_body(bq_resp[0], req_params)

        elif len(bq_resp) > 1:
            nums = [d.get('nta_corporate_num') for d in bq_resp]

            if len(bq_resp) == nums.count(nums[0]):
                resp_body = get_response_body(bq_resp[0], req_params)

            else:
                resp_body = set_response_body(req_params, bq_resp)
        else:
            resp_body  = {}

        return make_response(
            json.dumps(resp_body, indent=4),
            200,
            {'Content-Type': 'application/json'})

    except Exception as e:
        return Response("Exception: {}. Please contact techDev0".format(e), 500)

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

def get_response_body(bq_resp, req_params): # TODO: Consider a better name
    db_resp  = fetch_master_data(bq_resp)

    if len(db_resp):
        return  set_response_body(req_params, db_resp, 'db')
    else:
        insert(req_params, bq_resp)

        return set_response_body(req_params, bq_resp, 'bq')

def fetch_master_data(req_params):
    """ Fetch official unique company name from database

    Arg:
        req_params (dict): Http request parameters using search (database or BigQuery) or identify name string.

    Return:
        list: fetch result. converted SQLAlchemy.ResultProxy obj to list obj
    """
    result = select(req_params)

    return result.fetchall() # convert SQLAlchemy.ResultProxy obj to list obj

def set_response_body(req_params, resp, ref=None):
    resp_body = list()

    if ref == 'db':
        for row in resp:
            resp_body.append({
                "target_name": req_params['target_name'],
                "sys_master_id": req_params['sys_master_id'],
                "nta_corporate_num": row['nta_corporate_num'],
                "nta_corporate_name": row['nta_corporate_name'],
                "status": "registered."
            })
    elif ref == 'bq':
        resp_body.append({
            "target_name": req_params['target_name'],
            "sys_master_id": req_params['sys_master_id'],
            "nta_corporate_num": resp['nta_corporate_num'],
            "nta_corporate_name": resp['nta_corporate_name'],
            "status": "initial registered."
        })
    else:
        for v in resp:
            resp_body.append({
                "target_name": req_params['target_name'],
                "sys_master_id": req_params['sys_master_id'],
                "nta_corporate_num": v['nta_corporate_num'],
                "candidate_name": v['nta_corporate_name'],
                "status": "candidate names found."
            })

    return resp_body

def identify_company_name(target_name):
    """ Identify company name by JCL dictionary

    Arg:
        target_name (str): target name
    Return:
        dict: Identified unique name or candidate names from JCL dictionary into BigQuery
    """
    fmt_name_str = fmt_string(target_name)
    bq_resp = fetch_company_name_dic_bq(fmt_name_str, '=')

    if bq_resp.total_rows == 0:
        bq_resp = fetch_company_name_dic_bq(fmt_name_str, 'LIKE')

    result = list()

    if bq_resp.total_rows == 1:
        for i, row in enumerate(bq_resp):
            result.append({
                "nta_corporate_num": row.get('nta_corporate_num'),
                "process": row.get('process'),
                "nta_corporate_name": row.get('nta_corporate_name'),
                "candidate_company_name": row.get('candidate_company_name')})

    elif bq_resp.total_rows > 1:
        for i, row in enumerate(bq_resp):
            result.append({
                "nta_corporate_num": row.get('nta_corporate_num'),
                "process": row.get('process'),
                "nta_corporate_name": row.get('nta_corporate_name'),
                "candidate_company_name": row.get('candidate_company_name')})

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

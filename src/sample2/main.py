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
    normalized_name_str = unicodedata.normalize('NFKC', target_name)
    _fmt_name_str = delete_municipalities_str(delete_brackets(normalized_name_str))


    # 中途半端カッコの除去
    substr_idx = re.search(r"（", _fmt_name_str)
    if substr_idx != None and substr_idx.start() > 0:
        _fmt_name_str = target_name[0:substr_idx.start()]
    substr_idx = re.search(r"^(?!（).+）", target_name)
    if substr_idx != None and substr_idx.start() == 0:
        _fmt_name_str = target_name[substr_idx.end():]

    return _fmt_name_str

def eval_company_name(fmt_name_str):
    return get_company_names_dic()
    company_names_dic   = get_company_names_dic()
    part_match_max_len  = 14
    part_match_len      = len(fmt_name_str)

    """ TODO: WIP
    result = ''

    if _fmt_name_str not in company_names_dic.index:
        re_fmt_name_str = re.sub(r'[-・ 　‐]', '', _fmt_name_str) # ノイズとなりそうな文字を除去して再チェック(いらないかも)

        if re_fmt_name_str in company_names_dic.index:
            result = company_names_dic.loc[re_fmt_name_str, 'value']
        elif part_match_len >= part_match_max_len:
            # 一定文字数[target_max_len]以上の場合のみ、部分一致で検索
            result = search_partial_match(_fmt_name_str)
    else:
        result = company_names_dic.loc[target_name, 'value']

    if result == '': result = None"""

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
    # return company_name

def get_company_names_dic(target_name):
    client    = bigquery.Client()
    # query_job = client.query("SELECT string_field_0 FROM `dac-techdev0.jcl_dic.jcl_dic` WHERE string_field_0 = {}".format(target_name))
    query_job = client.query('SELECT string_field_0 FROM `dac-techdev0.jcl_dic.jcl_dic`')
    result    = query_job.result()  # Waits for job to complete.

    return result

def delete_municipalities_str(target_name_str): # municipalities = all of cities, wards, towns and villages
    municipalities_str = ['役所', '役場', '庁', '生協']

    for delete_str in municipalities_str:
        if re.search(delete_str, target_name_str):
            return target_name_str.replace(delete_str, '')

def delete_brackets(check_name):
    table = {
        "(": "（",
        ")": "）",
        "<": "＜",
        ">": "＞",
        "{": "｛",
        "}": "｝",
        "[": "［",
        "]": "］"
    }

    l = ['（[^（|^）]*）', '【[^【|^】]*】', '＜[^＜|^＞]*＞', '［[^［|^］]*］',
        '「[^「|^」]*」', '｛[^｛|^｝]*｝', '〔[^〔|^〕]*〕', '〈[^〈|^〉]*〉']
    for l_ in l:
        check_name = re.sub(l_, "", check_name)

    return delete_brackets(check_name) if sum([1 if re.search(l_, check_name) else 0 for l_ in l]) > 0 else check_name

def search_partial_match(self, check_name):
    company_name = ''

    ##部分一致
    check_result = self.company_name_list.loc[self.company_name_list.index.str.startswith(check_name), 'value']

    # 必要であれば、取得してきたvalueの精査をする
    if len(check_result) >= 1:
        company_name = str(check_result[0])
    return company_name

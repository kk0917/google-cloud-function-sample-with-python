import json
import os
import re
import unicodedata

from lib.sql_connection import insert, select
from google.cloud import bigquery

DATASET_ID = os.environ['BIGQUERY_DATASET_ID']
TABLE_ID   = os.environ['BIGQUERY_TABLE_ID']

def main(request):
    req_params = setRequestParams(request)
    resp       = getMasterData(req_params)

    if resp != None:
        return convert_master_data2json(resp)
    else:
        return json.dumps({"line_num": "19"}, indent=4)
        result = identify_company_name(req_params['target_name'])
        return convert_to_json(result)

def setRequestParams(request):
    return {
        "sys_id": request.args.get('sys_id'),
        "sys_master_id": request.args.get('sys_master_id'),
        "target_name": request.args.get('target_name')
    }

def getMasterData(req_params):
    return select(req_params)

def convert_master_data2json(resp):
    dicts = {}
    for i, row in enumerate(resp):
        _dict = {
            "id":          row.id,
            "unique_name": row.unique_name
        }

        dicts.setdefault(i, _dict)

    return json.dumps(dicts, indent=4)

def identify_company_name(target_name):
    fmt_name_str = fmt_string(target_name)
    company_name_ = evaluate_company_name(fmt_name_str)

def fmt_name_string(target_name): # TODO: fix inappropriate variables name
    normalized_name_str = unicodedata.normalize('NFKC', target_name)

    return delete_municipalities_str(delete_brackets(normalized_name_str))

def evaluate_company_name(_fmt_name_str):
    company_names_dic   = getCompanyNamesDic()
    part_match_max_len  = 14
    part_match_len      = len(_fmt_name_str)

    result = None

    if _fmt_name_str not in company_names_dic.index:
        re_fmt_name_str = re.sub(r'[-・ 　‐]', '', _fmt_name_str) # ノイズとなりそうな文字を除去して再チェック(いらないかも)

        if re_fmt_name_str in company_names_dic.index:
            result = company_names_dic.loc[re_fmt_name_str, 'value']
        elif part_match_len >= part_match_max_len:
            # 一定文字数[target_max_len]以上の場合のみ、部分一致で検索
            result = search_partial_match(_fmt_name_str)
    else:
        result = company_names_dic.loc[target_name, 'value']

    if result == '': result = None

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
    return company_name

def getCompanyNamesDic():
    client    = bigquery.Client()
    query_job = client.query(
        """
        SELECT
            string_field_0
        FROM `dac-techdev0:jcl_dic.jcl_dic`
        WHERE tags like '%google-bigquery%'
        ORDER BY view_count DESC
        LIMIT 10"""
    )

    results = query_job.result()  # Waits for job to complete.

    for row in results:
        print("{} : {} views".fmt(row.url, row.view_count))


def delete_municipalities_str(self, check_name): # municipalities = all of cities, wards, towns and villages
    municipalities_str = ['役所', '役場', '庁', '生協']

    for office_string in municipalities_str:
        check_pattern = r'.*({0})$'.fmt(office_string)
        if re.search(office_string, check_name):
            check_name = check_name.replace(office_string, '')
            break
    return check_name

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
    for key in table.keys():
        check_name = check_name.replace(key, table[key])

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

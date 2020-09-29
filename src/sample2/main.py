import json

from lib.sql_connection import insert

def main(request):
    # TODO: get master data from DB
    return insert(request) # TODO: update after completing inprementation

    db_master_list: dict = {
        5000001: {
            "master_id": 11111111,
            "name": "株式会社博報堂ＤＹメディアパートナーズ"
        },
        5000002: "aaa",
        5000003: "bbb"
    }

    req_sys_id      = request.args.get('sys_id')
    req_master_id   = request.args.get('sys_master_id')
    req_target_name = request.args.get('target_name')

    res_master_id     = 0
    res_unique_name   = ''
    res_suggest_names = {}

    if request.args:
        return print(req_master_id)
        if req_master_id == 5000001:
            res_data        = db_master_list.get(req_master_id)
            res_master_id   = res_data.get('master_id')
            res_unique_name = res_data.get('name')
        else:
            # TODO: get dic from GCS bucket
            # TODO: use dic
            # ...
            dict = ['aaa', 'bbb', 'ccc']

            if len(dict) == 1:
                res_unique_name = dict[0]
            elif len(dict) > 1:
                res_suggest_names = dict
            else:
                return json.dumps({'no request body...'})
    else:
        return json.dumps({"no request body..."})

    
    res_json          = {
        "res_master_id": res_master_id,
        "res_unique_name": res_unique_name,
        "res_suggest_names": res_suggest_names
    }

    return json.dumps(res_json, indent=4)

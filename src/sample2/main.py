import json

from lib.sql_connection import insert, select

def main(request):
    req_params = {
        "sys_id": request.args.get('sys_id'),
        "sys_master_id": request.args.get('sys_master_id'),
        "target_name": request.args.get('target_name')
    }
    # return json.dumps(req_params, indent=4)

    # TODO: select master data from DB
    resp  = select(req_params)

    return convert_to_json(resp)

    # return insert(request) # TODO: update after completing inprementation

    # TODO: get master data from DB and check it if exists

    # TODO: check flow

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

def convert_to_json(resp):
    dicts = {}
    for i, row in enumerate(resp):
        _dict = {
            "id":          row.id,
            "unique_name": row.unique_name
        }

        dicts.setdefault(i, _dict)

    return dicts
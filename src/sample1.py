import json

def main(request):
    # TODO: get master data from DB
    db_master_list = {
        5000001: {
            "master_id": 11111111,
            "name": "株式会社博報堂ＤＹメディアパートナーズ"
        },
        5000002: "aaa",
        5000003: "bbb"
    }

    req_sys_id    = request.args.get('req_sys_id')
    req_master_id = request.args.get('req_master_id')
    req_name      = request.args.get('req_name')

    res_master_id     = 0
    res_unique_name   = ''
    res_suggest_names = {}

    if request.args:
        if int(req_master_id) == 5000001:
            res_data        = db_master_list.get(str(req_master_id))
            res_master_id   = res_data.get('master_id')
            res_unique_name = res_data.get('name')
        else:
            # TODO: get dic from GCS bucket
            # TODO: use dic
            # ...
            dic_result = ['aaa', 'bbb', 'ccc']

            if len(dic_result) == 1:
                res_unique_name = dic_result[0]
            elif len(dic_result) > 1:
                res_suggest_names = dic_result
    else:
        return json.dumps({"no request data..."})

    
    res_json          = {
        "res_master_id": res_master_id,
        "res_unique_name": res_unique_name,
        "res_suggest_names": res_suggest_names
    }

    return json.dumps(res_json, indent=4)

# TODO: ...
def checkIsExistMasterName(request):
    request_json = request.get_json()

if __name__ == "__main__":
    print('Hello, world!')
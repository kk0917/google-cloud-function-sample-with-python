import os

import sqlalchemy
from sqlalchemy import create_engine

# Set the following variables depending on your specific
# connection name and root password from the earlier steps:
CONNECTION_NAME  = os.environ["CLOUD_SQL_CONNECTION_NAME"]
DB_USER          = os.environ['DB_USER']
DB_USER_PASSWORD = os.environ['DB_USER_PASSWORD']
DB_NAME          = os.environ['DB_NAME']
DRIVER_NAME      = 'postgres+pg8000'
query_string     = dict({"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(CONNECTION_NAME)})

engine = create_engine(
    sqlalchemy.engine.url.URL(
        drivername=DRIVER_NAME,
        username=DB_USER,
        password=DB_USER_PASSWORD,
        database=DB_NAME,
        query=query_string,
    ),
    pool_size=5,
    max_overflow=2,
    pool_timeout=30,
    pool_recycle=1800)

def insert(req_params, _bq_resp):
    return connect('INSERT', req_params, _bq_resp)

def select(params):
    return connect('SELECT', params)

def connect(type, params, bq_resp=None):
    try:
        with engine.connect() as conn:
            result = None

            if type == 'INSERT':
                trans  = conn.begin()
                # TODO: Write the query in hard code as a provisional response
                result = conn.execute("INSERT INTO master.m_corporation (nta_corporate_num, process, nta_corporate_name) VALUES ({}, {}, '{}')".format(bq_resp['nta_corporate_num'], bq_resp['process'], bq_resp['nta_corporate_name']))
                result = conn.execute("INSERT INTO master.corporation_externalsys_assignment (nta_corporate_num, external_sys_id, external_sys_master_id) VALUES ({}, {}, '{}')".format(bq_resp['nta_corporate_num'], params['sys_id'], params['sys_master_id']))
                trans.commit()
            elif type == 'SELECT':
                # TODO: Write the query in hard code as a provisional response
                result = conn.execute("SELECT * FROM master.corporation_externalsys_assignment AS assign LEFT JOIN master.m_corporation AS m_corp ON assign.nta_corporate_num = m_corp.nta_corporate_num WHERE m_corp.nta_corporate_num = {} AND m_corp.is_deleted = false".format(params['nta_corporate_num']))

            conn.close()

            return result

    except Exception as e:
        return 'Error: {}'.format(str(e))

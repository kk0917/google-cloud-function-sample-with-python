import os
import datetime

import sqlalchemy
from sqlalchemy import Table, Column, MetaData, Integer, String, Boolean, DateTime, Sequence, create_engine
from sqlalchemy.sql import and_

# Set the following variables depending on your specific
# connection name and root password from the earlier steps:
CONNECTION_NAME  = os.environ["CLOUD_SQL_CONNECTION_NAME"]
DB_USER          = os.environ['DB_USER']
DB_USER_PASSWORD = os.environ['DB_USER_PASSWORD']
DB_NAME          = os.environ['DB_NAME']
DRIVER_NAME      = 'postgres+pg8000'
query_string     = dict({"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(CONNECTION_NAME)})

DB_SCHEMA_NAME = 'master'
TABLE_NAME     = 'm_company'

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

metadata  = MetaData()

m_company = Table('m_company', metadata,
    Column('id', Integer, Sequence(DB_SCHEMA_NAME + '.' + TABLE_NAME + '_id_seq', metadata=metadata), primary_key=True),
    Column('sys_id', Integer),
    Column('sys_master_id', Integer),
    Column('unique_name', String(50)),
    Column('is_deleted', Boolean),
    Column('created_at', DateTime, onupdate=datetime.datetime.now),
    Column('updated_at', DateTime, onupdate=datetime.datetime.now),
    Column('deleted_at', DateTime, onupdate=datetime.datetime.now),
    schema=DB_SCHEMA_NAME)

def insert(req_params, identified_name):
    stmt = build_query('INSERT', req_params, identified_name)

    if (stmt != None):
        return connect('INSERT', stmt)
    else:
        return None

def select(req):
    stmt = build_query('SELECT', req)

    if (stmt != None):
        return connect('SELECT', stmt)
    else:
        return None

def build_query(type: str, _req_params, _identified_name=None):
    if (type == 'INSERT'):
        return m_company.insert().values({
            m_company.c.sys_id:        _req_params['sys_id'],
            m_company.c.sys_master_id: _req_params['sys_master_id'],
            m_company.c.unique_name:   _identified_name})

    elif (type == 'SELECT'):
        return m_company.select().where(and_(
            m_company.c.sys_id        == _req_params['sys_id'],
            m_company.c.sys_master_id == _req_params['sys_master_id'],
            m_company.c.is_deleted    == False))
    else:
        return None

def connect(type, stmt):
    try:
        with engine.connect() as conn:
            result = None

            if type == 'INSERT': # TODO: resolve errors
                trans  = conn.begin()
                result = conn.execute(stmt)
                trans.commit()
            elif type == 'SELECT':
                result = conn.execute(stmt)

            conn.close()

            return result
    except Exception as e:
        return 'Error: {}'.format(str(e))

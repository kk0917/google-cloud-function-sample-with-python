import os
import datetime

import sqlalchemy
from sqlalchemy import Table, Column, MetaData, Integer, String, Boolean, DateTime, Sequence, create_engine
from sqlalchemy.sql import and_

# Set the following variables depending on your specific
# connection name and root password from the earlier steps:
connection_name = os.environ["CLOUD_SQL_CONNECTION_NAME"]
db_password     = os.environ['DB_USER_PASSWORD']
db_name         = os.environ['DB_NAME']
db_user         = os.environ['DB_USER']
driver_name     = 'postgres+pg8000'
query_string    = dict({"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(connection_name)})

engine = create_engine(
        sqlalchemy.engine.url.URL(
            drivername=driver_name,
            username=db_user,
            password=db_password,
            database=db_name,
            query=query_string,
        ),
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800)

metadata  = MetaData()

# TODO: Replace to models/CorpInfoMaster.py
corp_info_master = Table('corp_info_master', metadata,
    Column('id', Integer, Sequence('corp_info_master_id_seq'), primary_key=True),
    Column('sys_id', Integer),
    Column('sys_master_id', Integer),
    Column('unique_name', String(50)),
    Column('is_deleted', Boolean),
    Column('created_at', DateTime, onupdate=datetime.datetime.now),
    Column('updated_at', DateTime, onupdate=datetime.datetime.now),
    Column('deleted_at', DateTime, onupdate=datetime.datetime.now))

def insert(req):
    # stmt = sqlalchemy.text('INSERT INTO public.corp_info_master (sys_id, sys_master_id, unique_name) VALUES (10001, 2000002, デジタルアドバタイジングコンソーシアム株式会社)')
    stmt = build_query('INSERT', req)

    if (stmt != None):
        return connect(stmt)
    else:
        return 'faild build query...'

def select(req):
    stmt = build_query('SELECT', req)

    if (stmt != None):
        return connect(stmt)
    else:
        return None

def build_query(type: str, _req):
    if (type == 'INSERT'):
        return corp_info_master.insert().values(
            sys_id=_req.args.get('sys_id'),
            sys_master_id=_req.args.get('sys_master_id'),
            # unique_name=_req.args.get('unique_name'))
            unique_name='***株式会社')
    elif (type == 'SELECT'):
        return corp_info_master.select().where(and_(
            corp_info_master.c.sys_id        == _req['sys_id'],
            corp_info_master.c.sys_master_id == _req['sys_master_id'],
            corp_info_master.c.is_deleted    != True))
    else:
        return None

def connect(stmt):
    try:
        with engine.connect() as conn:
            return conn.execute(stmt)
    except Exception as e:
        return 'Error: {}'.format(str(e))

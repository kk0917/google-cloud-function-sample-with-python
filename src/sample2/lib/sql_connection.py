import os
import datetime

from flask import Request
import sqlalchemy
from sqlalchemy import Table, Column, MetaData, Integer, String, Boolean, DateTime, Sequence, create_engine
from sqlalchemy.dialects.postgresql import insert

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

CorpInfoMaster = Table('corp_info_master', metadata,
    Column('id', Integer, Sequence('corp_info_master_id_seq'), primary_key=True),
    Column('sys_id', Integer),
    Column('sys_master_id', Integer),
    Column('unique_name', String(50)),
    Column('is_deleted', Boolean),
    Column('created_at', DateTime, onupdate=datetime.datetime.now),
    Column('updated_at', DateTime, onupdate=datetime.datetime.now),
    Column('deleted_at', DateTime, onupdate=datetime.datetime.now))

def build_query(type: str, req: Request):
    if (type == 'INSERT'):
        return CorpInfoMaster.insert().values(
            sys_id=req.sys_id,
            sys_master_id=req.sys_master_id,
            unique_name=req.unique_name)
    else:
        return None

def insert(engine, req: Request):
    # stmt = sqlalchemy.text('INSERT INTO public.corp_info_master (sys_id, sys_master_id, unique_name) VALUES (10001, 2000002, デジタルアドバタイジングコンソーシアム株式会社)')
    stmt = build_query('INSERT', req)

    if (stmt != None):
        try:
            with engine.connect() as conn:
                conn.execute(stmt)
        except Exception as e:
            return 'Error: {}'.format(str(e))
        return 'exit...'

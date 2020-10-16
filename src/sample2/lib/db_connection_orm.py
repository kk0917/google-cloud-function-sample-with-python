import os

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.sql import and_
from sqlalchemy.orm import sessionmaker

from models.MCompany import MCompany

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

def insert(params, identified_name):
    return connect('INSERT', params, identified_name)

def select(params):
    return connect('SELECT', params)

def connect(type, params, _identified_name=None):
    try:
        Session = sessionmaker(bind=engine)
        session = Session()

        result = None

        if type == 'INSERT':
            obj = MCompany(
                sys_id        = params['sys_id'],
                sys_master_id = params['sys_master_id'],
                unique_name   = _identified_name)

            session.add(obj)
            result = session.commit()

        elif type == 'SELECT':
            result = session.query(MCompany).filter(and_(
                MCompany.sys_id        == params['sys_id'],
                MCompany.sys_master_id == params['sys_master_id'],
                MCompany.is_deleted    == False)).all()

        session.close()

        return result

    except Exception as e:
        return 'Error: {}'.format(str(e))

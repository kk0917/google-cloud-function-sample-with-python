import datetime

from sqlalchemy import Sequence, Column, Integer, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.functions import current_timestamp

DB_SCHEMA_NAME = 'master'
TABLE_NAME     = 'm_company'

Base = declarative_base()

class MCompany(Base):
    __tablename__ = TABLE_NAME
    id            = Column(Integer, Sequence(DB_SCHEMA_NAME + '.' + TABLE_NAME + '_id_seq'), primary_key=True)
    sys_id        = Column(Integer, nullable=False)
    sys_master_id = Column(Integer, nullable=False)
    unique_name   = Column(String,  nullable=False, unique=True)
    is_deleted    = Column(Boolean, nullable=False, server_default='f')
    created_at    = Column(DateTime, onupdate=datetime.datetime.now, nullable=False, server_default=current_timestamp())
    updetad_at    = Column(DateTime, onupdate=datetime.datetime.now, nullable=False, server_default=current_timestamp())
    deleted_at    = Column(DateTime, onupdate=datetime.datetime.now, nullable=False, server_default=current_timestamp())
    schema        = DB_SCHEMA_NAME
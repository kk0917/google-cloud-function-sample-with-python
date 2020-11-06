import os

from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.functions import current_timestamp

DB_SCHEMA_NAME = os.environ['DB_SCHEMA_NAME']
TABLE_NAME     = os.environ['TABLE_NAME']

Base = declarative_base()

class MCorporation(Base):
    __tablename__ = DB_SCHEMA_NAME + '.' + TABLE_NAME
    nta_corporate_num = Column(Integer(13), primary_key=True, unique=True)
    unique_name       = Column(String, nullable=False)
    status            = Column(String(2), nullable=False)
    is_deleted        = Column(Boolean, nullable=False, server_default='f')
    created_at        = Column(DateTime, onupdate=current_timestamp(), nullable=False, server_default=current_timestamp())
    updetad_at        = Column(DateTime, onupdate=current_timestamp(), nullable=False, server_default=current_timestamp())
    deleted_at        = Column(DateTime, onupdate=current_timestamp(), nullable=False, server_default=current_timestamp())
    schema            = DB_SCHEMA_NAME

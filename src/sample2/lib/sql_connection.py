import os
import sqlalchemy

# Set the following variables depending on your specific
# connection name and root password from the earlier steps:
connection_name = os.environ["CLOUD_SQL_CONNECTION_NAME"]
db_password     = os.environ['DB_USER_PASSWORD']
db_name         = os.environ['DB_NAME']
db_user         = os.environ['DB_USER']
driver_name     = 'postgres+pg8000'
query_string    = dict({"unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(connection_name)})

def insert(request):
    request_json = request.get_json()
    stmt = sqlalchemy.text('INSERT INTO entries (guestName, content) values ("third guest", "Also this one");')
    db = sqlalchemy.create_engine(
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
        pool_recycle=1800
    )
    try:
        with db.connect() as conn:
            conn.execute(stmt)
    except Exception as e:
        return 'Error: {}'.format(str(e))
    return 'ok'

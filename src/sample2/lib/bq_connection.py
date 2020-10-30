import os

from google.cloud import bigquery

DATASET_ID = os.environ['BIGQUERY_DATASET_ID']
TABLE_ID   = os.environ['BIGQUERY_TABLE_ID']

def fetch_company_name_dic_bq(target_name, where_clause):
    client = bigquery.Client()

    if where_clause == '=':
        query_job = client.query('SELECT * FROM `dac-techdev0.{}.{}` WHERE candidate_company_name = "{}"'.format(DATASET_ID, TABLE_ID, target_name))

    elif where_clause == 'LIKE':
        query_job = client.query('SELECT * FROM `dac-techdev0.{}.{}` WHERE candidate_company_name LIKE ("%{}%")'.format(DATASET_ID, TABLE_ID, target_name))
    else:
        query_job = {}

    return query_job.result()
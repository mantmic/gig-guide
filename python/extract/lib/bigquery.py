import config as config
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import json

bigquery_client = bigquery.Client()
dataset = bigquery_client.dataset(config.bigquery_dataset_id)


def get_query(query_string):
    '''
    Function to query bigquery and result result as list of objects

    Args:
        query_string (str): SQL query to run against bigquery
    Returns:
        List
    '''
    query_job = bigquery_client.query(query_string)
    df = query_job.to_dataframe()
    return(json.loads(df.to_json(orient='records')))



def load_json_data(data,table):
    '''
    Loads JSON data into bigquery
    '''
    if(len(data) == 0):
        print("No results to push")
        return()
    bq_table = dataset.table(table)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
    ]
    job = bigquery_client.load_table_from_json(data, bq_table, job_config = job_config)
    return(job.result())

def check_table_exists(table_name):
    '''
    Function that checks whether a table exists

    Args:
        table_name (str)
            The name of the table to check the existance of

    Returns:
        bool: True if table exists, False otherwise
    '''
    table_ref = dataset.table(table_name)
    try:
        bigquery_client.get_table(table_ref)
        return True
    except NotFound:
        return False

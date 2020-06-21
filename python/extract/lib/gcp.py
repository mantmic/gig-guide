import config as config
from google.cloud import bigquery
from google.cloud import storage

from google.cloud.exceptions import NotFound
import json
import uuid

bigquery_client = bigquery.Client()
storage_client = storage.Client()

gcp_bucket_landing = storage_client.get_bucket(config.gcp_bucket_landing)

dataset = bigquery_client.dataset(config.bigquery_dataset_id)

def get_file_path(source_system,table,timestamp):
    # convert timestamp to string
    timestamp_string = timestamp.isoformat().replace(':','').replace('.','')
    return(f"gs://{config.gcp_bucket_landing}/{source_system}/{table}/{table}_{timestamp_string}")

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



def _load_json_data(data,table,directory):
    '''
    Loads JSON data into GCP and create external dataset in Bigquery
    '''
    if(len(data) == 0):
        print("No results to push")
        return()
    # cast to and from json to cast all fields to strings
    converted_data = json.loads(json.dumps(data),parse_float=str, parse_int=str, parse_constant=str)
    bq_table = dataset.table(table)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
    ]
    job = bigquery_client.load_table_from_json(converted_data, bq_table, job_config = job_config)
    return(job.result())


def create_external_table(directory,table_name):
    table = bigquery.Table(dataset.table(table_name))

    external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")

    external_config.source_uris = [
        "gs://{}/{}/*".format(config.gcp_bucket_landing,directory)
    ]
    external_config.maxBadRecords = 10000

    external_config.autodetect = True

    table.external_data_configuration = external_config

    # drop table if exists
    bigquery_client.delete_table('.'.join([config.bigquery_dataset_id,table_name]), not_found_ok=True)
    # create table
    create_table = bigquery_client.create_table(table)  # API request
    return(create_table)

def load_json_data(data,directory,file_prefix):
    '''
    Loads JSON data into GCP and create external dataset in Bigquery
    '''
    if(len(data) == 0):
        print("No results to push")
        return()
    # append a random string to the file name to avoid conflicts
    file_name = '_'.join([file_prefix,str(uuid.uuid1())])
    # get full path
    file_path = '/'.join([directory,file_name])
    print("Pushing data to %s" % file_path)
    blob = gcp_bucket_landing.blob(file_path)
    # convert data to newline delimited json
    converted_data = "\n".join([json.dumps(r) for r in data])
    job = blob.upload_from_string(converted_data)
    # push to cloud storage
    #job = bigquery_client.load_table_from_json(converted_data, bq_table, job_config = job_config)
    return(job)



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

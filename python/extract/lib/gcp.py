import config as config
import lib.util as util 

from google.cloud import bigquery
from google.cloud import storage

from google.cloud.exceptions import NotFound
import json
import uuid
import datetime

extract_ts = datetime.datetime.now().isoformat()

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

def get_blob_data(file_path):
    blob = gcp_bucket_landing.blob(file_path)
    data = []
    blob_string = blob.download_as_string()
    for row in blob_string.decode("utf-8").split('\n'):
        data.append(json.loads(row))
    return(data)

def create_external_table(directory,table_name):
    table = bigquery.Table(dataset.table(table_name))

    external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")

    external_config.source_uris = [
        "gs://{}/{}/*".format(config.gcp_bucket_landing,directory)
    ]
    external_config.max_bad_records = 10000
    #external_config.ignoreUnknownValues = True
    external_config.autodetect = True

    table.external_data_configuration = external_config

    # drop table if exists
    bigquery_client.delete_table('.'.join([config.bigquery_dataset_id,table_name]), not_found_ok=True)
    # create table
    create_table = bigquery_client.create_table(table)  # API request
    return(create_table)

def preprocess_row(row):
    # remove empty lists 
    util.remove_empty_lists(row)
    row['extract_ts'] = extract_ts
    return(row)

def push_data_blob(data,file_path):
    print("Pushing data to %s" % file_path)
    blob = gcp_bucket_landing.blob(file_path)
    # convert data to newline delimited json
    converted_data = "\n".join([json.dumps(r) for r in data])
    job = blob.upload_from_string(converted_data)
    return(job)

def blob_to_table(file_path,table_id):
    '''
        Loads data from blob storage at the given file_path into the table_id 
    '''
    print("Pushing file {} to {}".format(file_path,table_id))
    # pull table specific config 
    table_config = config.bigquery_load_table_parameters.get(table_id,{})
    # get table 
    table_ref = dataset.table(table_id)
    # start jon config 
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    # if there is a schema specified for the table, use it
    if(table_config.get('schema')):
        job_config.schema = table_config.get('schema')
    else:
        job_config.autodetect = table_config.get('autodetect',True)
    job_config.max_bad_records = 10000
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
    ]
    uri = "gs://{}/{}".format(config.gcp_bucket_landing,file_path)
    # start job
    load_job = bigquery_client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))
    # get results 
    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = bigquery_client.get_table(table_ref)
    print("Loaded {} rows.".format(destination_table.num_rows))
    return(load_job)


def reprocess_blob_data():
    blobs = gcp_bucket_landing.list_blobs()

    for blob in blobs:
        blob.name
        data = get_blob_data(blob.name)
        for d in data:
            util.remove_empty_lists(d)
        push_data_blob(data,blob.name)

def reload_landing_data(start_file = ''):
    blobs = gcp_bucket_landing.list_blobs()

    for blob in blobs:
        if(blob.name < start_file):
            continue
        file_path = blob.name
        table_id = '_'.join(file_path.split('/')[:-1])
        blob_to_table(file_path,table_id)

def load_json_data(data,directory,file_prefix):
    '''
    Loads JSON data into GCP and create external dataset in Bigquery
    '''
    if(len(data) == 0):
        print("No results to push")
        return()
    # preprocess rows 
    data = [preprocess_row(row) for row in data]
    # append a random string to the file name to avoid conflicts
    file_name = '_'.join([file_prefix,str(uuid.uuid1())])
    # get full path
    file_path = '/'.join([directory,file_name])
    # push to cloud storage
    job = push_data_blob(data,file_path)
    # push to table 
    table_id = '_'.join(file_path.split('/')[:-1])
    load_job = blob_to_table(file_path,table_id)
    #job = bigquery_client.load_table_from_json(converted_data, bq_table, job_config = job_config)
    return(load_job)



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

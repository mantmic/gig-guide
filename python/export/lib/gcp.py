
from google.cloud import bigquery
from google.cloud import storage

from google.cloud.exceptions import NotFound
import json
import os 

bigquery_client = bigquery.Client()
storage_client = storage.Client()

# get environment variables
project = os.getenv('GCP_PROJECT_ID', "melbourne-gig-guide")
location = os.getenv('GCP_LOCATION', "australia-southeast1")
dataset_id = os.getenv('BIGQUERY_DATASET_ID', "dev")
bucket_name = os.getenv('GCP_BUCKET_NAME', "melbourne-gig-guide-public-dev")

export_bucket = storage_client.get_bucket(bucket_name)

dataset = bigquery_client.dataset(dataset_id)


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


def load_json_data(data,file_path):
    '''
    Loads JSON data into GCP and create external dataset in Bigquery
    '''
    if(len(data) == 0):
        print("No results to push")
        return()
    print("Pushing data to %s" % file_path)
    blob = export_bucket.blob(file_path)
    # convert data to newline delimited json
    converted_data = json.dumps(data)
    job = blob.upload_from_string(converted_data)
    # push to cloud storage
    #job = bigquery_client.load_table_from_json(converted_data, bq_table, job_config = job_config)
    return(job)



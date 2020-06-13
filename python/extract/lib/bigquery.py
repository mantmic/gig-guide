import config as config
from google.cloud import bigquery

bigquery_client = bigquery.Client()
dataset = bigquery_client.dataset(config.bigquery_dataset_id)

def load_json_data(data,table):
    '''
    Loads JSON data into bigquery
    '''
    bq_table = dataset.table(table)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
    ]
    job = bigquery_client.load_table_from_json(data, bq_table, job_config = job_config)
    return(job.result())

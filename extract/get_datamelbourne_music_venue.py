import requests
from google.cloud import bigquery
import config

bigquery_client = bigquery.Client()
dataset = bigquery_client.dataset(config.bigquery_dataset_id)

table = dataset.table('datamelbourne_music_venue')

url = "https://data.melbourne.vic.gov.au/resource/mgqj-necz.json"

res = requests.get(url)

data = res.json()
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

job = bigquery_client.load_table_from_json(data, table, job_config = job_config)

print(job.result())

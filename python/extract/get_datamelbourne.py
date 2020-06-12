import requests
from google.cloud import bigquery
import config as config
import datetime
import prefect
from prefect import task
from prefect.engine.signals import SKIP
from prefect.tasks.shell import ShellTask

bigquery_client = bigquery.Client()
dataset = bigquery_client.dataset(config.bigquery_dataset_id)

@task
def get_music_venue() -> object:
    """
    Extracts and loads data from datamelbourne
    """
    url = "https://data.melbourne.vic.gov.au/resource/mgqj-necz.json"
    table = dataset.table('datamelbourne_music_venue')
    res = requests.get(url)
    data = res.json()
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    job = bigquery_client.load_table_from_json(data, table, job_config = job_config)

    return(job.result())

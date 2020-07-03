import os
from google.cloud import bigquery

# get environment variables
project = os.getenv('GCP_PROJECT_ID', "melbourne-gig-guide")
location = os.getenv('GCP_LOCATION', "australia-southeast1")
dataset_id = os.getenv('BIGQUERY_DATASET_ID', "dev")
bucket_name = os.getenv('GCP_BUCKET_NAME', "melbourne-gig-guide-public-dev")

# initialize client
client = bigquery.Client()

# list of tables to export
table_export_list = [
    {"table_id":"venue","file_name":"venue"},
    {"table_id":"artist","file_name":"artist"},
    {"table_id":"gig","file_name":"gig"}
]

for table in table_export_list:
    table_id = table.get('table_id')
    file_name = table.get('file_name')
    destination_uri = "gs://{}/{}".format(bucket_name, ".".join([file_name,'csv']))

    print(
        "Exporting {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    # define job config
    job_config = bigquery.ExtractJobConfig()
    job_config.destination_format = (bigquery.DestinationFormat.CSV)

    # extract data
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location=location,
        job_config=job_config
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )

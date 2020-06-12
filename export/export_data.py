from google.cloud import bigquery
client = bigquery.Client()


project = "melbourne-gig-guide"
dataset_id = "dev"
table_id = "venue"

bucket_name = "melbourne-gig-guide-public"

destination_uri = "gs://{}/{}".format(bucket_name, "venue.csv")

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
    location="australia-southeast1",
    job_config=job_config
)  # API request
extract_job.result()  # Waits for job to complete.

print(
    "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
)

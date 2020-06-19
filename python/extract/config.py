import os
bigquery_dataset_id = os.getenv('BIGQUERY_DATASET_ID', "landing_dev")
gcp_bucket_landing = os.getenv('GCP_BUCKET_LANDING', "melbourne-gig-guide-landing-dev")

# extract type is either full or incremental. If full
extract_type = os.getenv('EXTRACT_TYPE', "incremental")

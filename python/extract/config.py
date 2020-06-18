import os
bigquery_dataset_id = os.getenv('BIGQUERY_DATASET_ID', "landing_dev")
gcp_bucket_landing = os.getenv('GCP_BUCKET_LANDING', "melbourne-gig-guide-landing-dev")

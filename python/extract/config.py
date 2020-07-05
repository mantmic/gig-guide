import os
bigquery_dataset_id = os.getenv('LANDING_DATASET_ID', "landing_dev")
gcp_bucket_landing = os.getenv('GCP_BUCKET_LANDING', "melbourne-gig-guide-landing-dev")

# extract type is either full or incremental. If full
extract_type = os.getenv('EXTRACT_TYPE', "incremental")

extract_days = int(os.getenv('GIG_EXTRACT_DAYS', 1))

# variable to offset the extract start day by X number days. Can be used to only extract gigs from X days into the future
extract_start_date_offset = int(os.getenv('EXTRACT_START_DATE_OFFSET', 0))

bigquery_load_table_parameters = {
    "spotify_artist_top_tracks":{
        "autodetect":False 
    }
}
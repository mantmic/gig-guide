import os
from google.cloud import bigquery

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
    },
    "oztix_gig_details":{
        "autodetect":False,
        "schema":[
            bigquery.SchemaField('extract_ts', 'TIMESTAMP', 'NULLABLE', 'bq-datetime', ()), 
            bigquery.SchemaField('event_location', 'STRING', 'NULLABLE', None, ()), 
            bigquery.SchemaField('event_venue_name', 'STRING', 'NULLABLE', None, ()), 
            bigquery.SchemaField('event_presenter', 'STRING', 'NULLABLE', None, ()), 
            bigquery.SchemaField('event_image_url', 'STRING', 'NULLABLE', None, ()), 
            bigquery.SchemaField('event_tickets', 'RECORD', 'REPEATED', None, (
                bigquery.SchemaField('ticket_tag', 'STRING', 'NULLABLE', None, ()), 
                bigquery.SchemaField('ticket_description', 'STRING', 'NULLABLE', None, ()), 
                bigquery.SchemaField('ticket_price', 'STRING', 'NULLABLE', None, ()), 
                bigquery.SchemaField('ticket_name', 'STRING', 'NULLABLE', None, ()))
            ), 
            bigquery.SchemaField('event_name', 'STRING', 'NULLABLE', None, ()), 
            bigquery.SchemaField('event_datetime', 'STRING', 'NULLABLE', None, ()), 
            bigquery.SchemaField('oztix_ticket_url', 'STRING', 'NULLABLE', None, ())
        ]
    }
}
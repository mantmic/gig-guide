{{ config(materialized='view') }}

select
    google_search_artist_id
  , artist_name 
  , bio_link_url 
from
  {{ ref('google_search_artist_details') }}
{{ config(materialized='table') }}
select distinct
    bandcamp_artist_id
  , bandcamp_artist_name
  , bandcamp_url
  , bandcamp_artist_location
  , bandcamp_artist_bio
from
  {{ ref('bandcamp_artist_details_combined') }}

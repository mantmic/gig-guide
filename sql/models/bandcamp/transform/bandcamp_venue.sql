{{ config(materialized='table') }}

select distinct
    bandcamp_venue_id
  , bandcamp_venue_location
  , bandcamp_venue_name
from
  {{ ref('bandcamp_gig') }}

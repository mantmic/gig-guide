{{ config(materialized='view') }}

select 
    reverbnation_venue_id
  , venue_reverbnation_url as reverbnation_url
  , venue_name 
  , venue_location_full 
  , venue_image_url 
from 
  {{ ref('reverbnation_venue_shows') }} 
where
  dup_rn = 1
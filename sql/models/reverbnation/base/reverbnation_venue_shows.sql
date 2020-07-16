{{ config(materialized='view') }}

select
    to_hex ( {{ dbt_utils.surrogate_key('venue_reverbnation_url') }} )  as reverbnation_venue_id 
  , venue_reverbnation_url  
  , trim ( venue_name )                                                 as venue_name 
  , venue_location_full 
  , venue_image_url 
  , venue_social_links
  , shows
  , extract_ts
  , row_number() over ( partition by venue_reverbnation_url order by extract_ts desc ) as dup_rn 
from  
  {{ source('reverbnation', 'reverbnation_venue_shows') }}
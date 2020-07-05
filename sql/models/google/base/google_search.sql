{{ config(materialized='view') }}

with dup_data as 
( select 
    to_hex ( {{ dbt_utils.surrogate_key('input_query') }} )                 as google_search_result_sk
  , input_query
  , to_hex ( {{ dbt_utils.surrogate_key('artist_name') }} )                 as google_search_artist_id 
  , trim ( artist_name )                                                    as artist_name
  , other_info
  , events
  , music_platform_links
  , bio_link_url 
  , social_media_links
  , row_number() over ( partition by input_query order by extract_ts desc ) as dup_rn
from 
  {{ source('google', 'google_search') }}
)
select 
    google_search_result_sk
  , input_query
  , google_search_artist_id 
  , artist_name
  , other_info
  , events
  , music_platform_links
  , social_media_links
  , bio_link_url 
from 
    dup_data 
where
    dup_rn = 1

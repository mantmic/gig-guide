{{ config(materialized='table') }}

with dup_data as 
( select 
    google_search_artist_id 
  , artist_name
  , other_info
  , events
  , music_platform_links
  , social_media_links
  , bio_link_url 
  , row_number() over ( partition by google_search_artist_id order by google_search_artist_id ) as dup_rn 
from 
  {{ ref('google_search') }}
where
  artist_name is not null
) 
select  
    google_search_artist_id 
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
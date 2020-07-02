{{ config(materialized='view') }}

select distinct
    artist.google_search_artist_id 
  , {{ clean_url('links.url') }}    as music_platform_url 
  , links.description               as music_platform_name 
from
  {{ ref('google_search_artist_details') }} artist 
  cross join
  unnest ( artist.music_platform_links ) as links 
  left outer join
  {{ ref('web_url_blacklist') }} blacklist
    on {{ clean_url('links.url') }} = blacklist.url
where
  blacklist.url is null
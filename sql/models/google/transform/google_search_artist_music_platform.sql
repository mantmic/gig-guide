{{ config(materialized='view') }}

select
    artist.google_search_artist_id 
  , {{ clean_url('links.url') }}    as music_platform_url 
  , links.description               as music_platform_name 
from
  {{ ref('google_search_artist_details') }} artist 
  cross join
  unnest ( artist.music_platform_links ) as links 
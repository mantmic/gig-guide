{{ config(materialized='view') }}

select
    artist.google_search_artist_id 
  , {{ clean_url('social.url') }}   as social_media_url
  , lower ( social.description )    as social_media_platform
from
  {{ ref('google_search_artist_details') }} artist 
  cross join
  unnest ( artist.social_media_links ) social 
  
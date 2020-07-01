{{ config(materialized='table') }}

select
    artist_details.unearthed_artist_id
  , {{ clean_url('social_links.link_url') }}    as social_link_url
  , social_links.link_type                      as social_link_type
from
  {{ ref('unearthed_artist_details') }} artist_details
  cross join
  unnest ( artist_details.social_links ) social_links

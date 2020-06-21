{{ config(materialized='table') }}

select distinct
    details.unearthed_artist_id
  , details.unearthed_artist_url        as unearthed_url
  , details.unearthed_artist_name
  , details.artist_location
  , details.artist_website              as artist_website_url
from
  {{ ref('unearthed_artist_details') }} details

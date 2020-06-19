{{ config(materialized='table') }}

select distinct
    details.unearthed_artist_details_sk as unearthed_artist_id
  , details.unearthed_artist_url        as unearthed_url
  , details.unearthed_artist_name
  , details.artist_location
  , search.unearthed_search_artist_name as search_artist_name
  , details.artist_website              as artist_website_url
from
  {{ ref('unearthed_artist_details') }} details
  join
  {{ ref('unearthed_artist_search') }} search
    using ( unearthed_artist_url )
where
  trim ( lower ( search.unearthed_search_artist_name ) ) = trim ( lower ( details.unearthed_artist_name  ) )

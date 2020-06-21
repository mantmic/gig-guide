{{ config(materialized='view') }}

with dup_data as
( select
    to_hex ( {{ dbt_utils.surrogate_key('unearthed_artist_url') }} )                  as unearthed_artist_id
  , unearthed_artist_url
  , trim ( artist_name )                                                              as unearthed_artist_name
  , location                                                                          as artist_location
  , website                                                                           as artist_website
  , track_ids                                                                         as unearthed_track_ids
  , socials                                                                           as social_links
  , row_number() over ( partition by unearthed_artist_url order by _FILE_NAME desc )  as dup_rn
from
  {{ source('unearthed', 'unearthed_artist_details') }}
where
  unearthed_artist_url is not null
)
select
    unearthed_artist_id
  , unearthed_artist_url
  , unearthed_artist_name
  , artist_location
  , artist_website
  , unearthed_track_ids
  , social_links
from
  dup_data
where
  dup_rn = 1

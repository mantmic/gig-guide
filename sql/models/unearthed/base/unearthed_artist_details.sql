{{ config(materialized='view') }}

with dup_data as
( select
    to_hex ( {{ dbt_utils.surrogate_key('unearthed_artist_url') }} )                  as unearthed_artist_details_sk
  , unearthed_artist_url
  , trim ( artist_name )                                                              as unearthed_artist_name
  , location                                                                          as artist_city
  , website                                                                           as artist_website
  , track_ids                                                                         as unearthed_track_ids
  , row_number() over ( partition by unearthed_artist_url order by _FILE_NAME desc )  as dup_rn
from
  {{ source('unearthed', 'unearthed_artist_details') }}
where
  unearthed_artist_url is not null
)
select
    unearthed_artist_details_sk
  , unearthed_artist_url
  , unearthed_artist_name
  , artist_city
  , artist_website
  , unearthed_track_ids
from
  dup_data
where
  dup_rn = 1

{{ config(materialized='view') }}

with dup_data as
( select
    to_hex ( {{ dbt_utils.surrogate_key('search_artist_name','search_result_order') }} )    as unearthed_search_artist_sk
  , search_artist_name                                                                      as unearthed_search_artist_name
  , search_result_type                                                                      as unearthed_search_result_type
  , unearthed_artist_url
  , search_result_order                                                                     as unearthed_search_result_order
  , rank() over ( partition by search_artist_name order by _FILE_NAME desc ) as dup_rn
from
  {{ source('unearthed', 'unearthed_artist_search') }}
)
select
    unearthed_search_artist_sk
  , unearthed_search_artist_name
  , unearthed_search_result_type
  , unearthed_artist_url
  , unearthed_search_result_order
from
  dup_data
where
  dup_rn = 1

{{ config(materialized='view') }}

with dup_data as
( select
    to_hex ( {{ dbt_utils.surrogate_key('input_artist_name', 'search_rank') }} )  as bandcamp_artist_search_sk
  , trim ( input_artist_name )                                                    as bandcamp_input_artist_name
  , trim ( bandcamp_artist_name )                                                 as bandcamp_artist_name
  , bandcamp_url
  , to_hex ( {{ dbt_utils.surrogate_key('bandcamp_url') }} )                      as bandcamp_artist_id
  , search_rank
  , case when trim ( lower ( input_artist_name ) ) = trim ( lower ( bandcamp_artist_name ) ) then 'Y'
         else 'N'
    end                                                                           as name_match_yn
  , rank() over ( partition by input_artist_name order by extract_ts desc )       as dup_rn
from
  {{ source('bandcamp', 'bandcamp_artist_search') }}
)
select
    bandcamp_artist_search_sk
  , bandcamp_artist_id
  , bandcamp_input_artist_name
  , bandcamp_artist_name
  , bandcamp_url
  , search_rank
  , name_match_yn
  , row_number() over ( partition by bandcamp_input_artist_name order by name_match_yn desc, search_rank ) as search_rank_order
from
  dup_data
where
  dup_rn = 1

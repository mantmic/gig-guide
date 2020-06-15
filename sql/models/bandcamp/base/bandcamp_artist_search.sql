{{ config(materialized='table') }}

with dup_data as
( select
    {{ dbt_utils.surrogate_key('input_artist_name', 'search_rank') }}       as bandcamp_artist_search_sk
  , input_artist_name                                                       as bandcamp_input_artist_name
  , trim ( bandcamp_artist_name )                                           as bandcamp_artist_name
  , bandcamp_url
  , search_rank
  , rank() over ( partition by input_artist_name order by extract_ts desc ) as dup_rn
from
  {{ source('bandcamp', 'bandcamp_artist_search') }}
)
select
    bandcamp_artist_search_sk
  , bandcamp_input_artist_name
  , bandcamp_artist_name
  , bandcamp_url
  , search_rank
  -- indicate whether the names match - keep this simple for now
  , case when trim ( lower ( bandcamp_input_artist_name ) ) = trim ( lower ( bandcamp_artist_name ) ) then 'Y'
         else 'N'
    end as name_match_yn
from
  dup_data
where
  dup_rn = 1

{{ config(materialized='table') }}
with ranked_results as
( select
    bandcamp_input_artist_name
  , bandcamp_artist_name
  , bandcamp_url
  , row_number() over ( partition by bandcamp_input_artist_name order by search_rank desc ) as search_rank_order
from
  {{ ref('bandcamp_artist_search') }}
where
  name_match_yn = 'Y'
)
select
    bandcamp_input_artist_name
  , bandcamp_artist_name
  , bandcamp_url
  , md5 ( bandcamp_url )        as bandcamp_artist_id
from
  ranked_results
where
  search_rank_order = 1

{{ config(materialized='table') }}

-- thebrag artists based on name matches
select
    to_hex ( {{ dbt_utils.surrogate_key('bandcamp_search.bandcamp_artist_name', 'thebrag.thebrag_artist_name', 'unearthed.unearthed_artist_id' ) }} )  as artist_id
  , thebrag.thebrag_artist_id
  , bandcamp_search.bandcamp_artist_id
  , unearthed.unearthed_artist_id
from
  {{ ref('thebrag_artist') }} thebrag
  left outer join
  -- get top matched bandcamp search result
  {{ ref('bandcamp_artist_search') }} bandcamp_search
    on lower ( thebrag.thebrag_artist_name ) = lower ( bandcamp_search.bandcamp_input_artist_name )
    and bandcamp_search.name_match_yn = 'Y'
    and bandcamp_search.search_rank_order = 1
  left outer join
  {{ ref('unearthed_artist_search_final') }} unearthed
    on lower ( thebrag.thebrag_artist_name ) = lower ( unearthed.search_artist_name )
    and unearthed.name_match_yn = 'Y'
    and unearthed.search_rank_order = 1

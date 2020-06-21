{{ config(materialized='table') }}

-- thebrag artists based on name matches
with name_match as
( select
    thebrag.thebrag_artist_id
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
)
, combined as
( select
    name_match.thebrag_artist_id
  , coalesce ( name_match.bandcamp_artist_id, unearthed.bandcamp_artist_id )    as bandcamp_artist_id
  , coalesce ( name_match.unearthed_artist_id, bandcamp.unearthed_artist_id )   as unearthed_artist_id
from
  name_match
  left outer join
  {{ ref('stg_artist_bandcamp_unearthed_match') }} bandcamp
    on name_match.bandcamp_artist_id  = bandcamp.bandcamp_artist_id
  left outer join
  {{ ref('stg_artist_bandcamp_unearthed_match') }} unearthed
    on name_match.unearthed_artist_id = unearthed.unearthed_artist_id
)
select
    to_hex ( {{ dbt_utils.surrogate_key('thebrag_artist_id', 'bandcamp_artist_id', 'unearthed_artist_id' ) }} )  as artist_id
  , *
from
  combined

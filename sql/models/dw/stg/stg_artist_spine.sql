{{ config(materialized='table') }}

-- thebrag artists based on name matches
with name_match as
( select
    thebrag.thebrag_artist_id
  , bandcamp_search.bandcamp_artist_id
  , unearthed.unearthed_artist_id
  , google.google_search_artist_id
  , spotify.spotify_artist_id
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
  left outer join 
  {{ ref('google_search_artist') }} google 
    on lower ( thebrag.thebrag_artist_name ) = lower ( google.artist_name )
  left outer join 
  {{ ref('spotify_artist_search_final') }} spotify 
    on lower ( thebrag.thebrag_artist_name ) = lower ( spotify.artist_name )
)
, combined as
( select distinct
    name_match.thebrag_artist_id
  , name_match.spotify_artist_id
  , coalesce ( name_match.bandcamp_artist_id, bandcamp_unearthed_u.bandcamp_artist_id, google_bandcamp_b.bandcamp_artist_id )               as bandcamp_artist_id
  , coalesce ( name_match.unearthed_artist_id, bandcamp_unearthed_b.unearthed_artist_id, google_unearthed_u.unearthed_artist_id )           as unearthed_artist_id
  , coalesce ( name_match.google_search_artist_id, google_unearthed_g.google_search_artist_id, google_bandcamp_g.google_search_artist_id )  as google_search_artist_id
from
  name_match
  left outer join
  {{ ref('stg_artist_bandcamp_unearthed_match') }} bandcamp_unearthed_b
    on name_match.unearthed_artist_id = bandcamp_unearthed_b.unearthed_artist_id
  left outer join
  {{ ref('stg_artist_bandcamp_unearthed_match') }} bandcamp_unearthed_u
    on name_match.bandcamp_artist_id  = bandcamp_unearthed_u.bandcamp_artist_id
  left outer join 
  {{ ref('stg_artist_google_bandcamp_match') }} google_bandcamp_g
    on name_match.bandcamp_artist_id = google_bandcamp_g.bandcamp_artist_id
  left outer join 
  {{ ref('stg_artist_google_bandcamp_match') }} google_bandcamp_b
    on name_match.google_search_artist_id = google_bandcamp_b.google_search_artist_id
  left outer join 
  {{ ref('stg_artist_google_unearthed_match') }} google_unearthed_g
    on name_match.unearthed_artist_id       = google_unearthed_g.unearthed_artist_id
  left outer join 
  {{ ref('stg_artist_google_unearthed_match') }} google_unearthed_u
    on name_match.google_search_artist_id   = google_unearthed_u.google_search_artist_id
)
select
    to_hex ( {{ dbt_utils.surrogate_key('thebrag_artist_id', 'bandcamp_artist_id', 'unearthed_artist_id','google_search_artist_id' ) }} )  as artist_id
  , *
from
  combined

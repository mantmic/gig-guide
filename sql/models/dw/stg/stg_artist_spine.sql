{{ config(materialized='table') }}

-- thebrag artists
select
    to_hex ( {{ dbt_utils.surrogate_key('bandcamp.bandcamp_artist_name', 'thebrag.thebrag_artist_name', 'unearthed.unearthed_artist_id' ) }} )  as artist_id
  , thebrag.thebrag_artist_id
  , bandcamp.bandcamp_artist_id
  , unearthed.unearthed_artist_id
from
  {{ ref('thebrag_artist') }} thebrag
  left outer join
  {{ ref('bandcamp_artist') }} bandcamp
    on lower ( thebrag.thebrag_artist_name ) = lower ( bandcamp.bandcamp_input_artist_name )
  left outer join
  {{ ref('unearthed_artist') }} unearthed
    on lower ( thebrag.thebrag_artist_name ) = lower ( unearthed.search_artist_name )

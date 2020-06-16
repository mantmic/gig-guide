{{ config(materialized='table') }}

-- thebrag artists
select
    to_hex ( {{ dbt_utils.surrogate_key('bandcamp.bandcamp_artist_name', 'thebrag.thebrag_artist_name') }} )  as artist_id
  , coalesce ( bandcamp.bandcamp_artist_name, thebrag.thebrag_artist_name )   as artist_name
  , thebrag.thebrag_artist_id
  , bandcamp.bandcamp_artist_id
from
  {{ ref('thebrag_artist') }} thebrag
  left outer join
  {{ ref('bandcamp_artist') }} bandcamp
    on thebrag.thebrag_artist_name = bandcamp.bandcamp_input_artist_name

{{ config(materialized='table') }}

select
    spine.artist_id
  , spine.artist_name
  , bandcamp.bandcamp_url
from
  {{ ref('stg_artist_spine') }} spine
  left outer join
  {{ ref('bandcamp_artist') }} bandcamp
    using ( bandcamp_artist_id )
  left outer join
  {{ ref('thebrag_artist') }} thebrag
    using ( thebrag_artist_id )

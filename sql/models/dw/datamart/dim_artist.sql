{{ config(materialized='table') }}

select
    spine.artist_id
  , coalesce ( unearthed.unearthed_artist_name, bandcamp.bandcamp_artist_name, thebrag.thebrag_artist_name ) as artist_name
  , bandcamp.bandcamp_url
  , unearthed.unearthed_url
  , unearthed.artist_website_url
  , unearthed.artist_location
from
  {{ ref('stg_artist_spine') }} spine
  left outer join
  {{ ref('bandcamp_artist') }} bandcamp
    using ( bandcamp_artist_id )
  left outer join
  {{ ref('thebrag_artist') }} thebrag
    using ( thebrag_artist_id )
  left outer join
  {{ ref('unearthed_artist') }} unearthed
    using ( unearthed_artist_id )

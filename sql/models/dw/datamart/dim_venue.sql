{{ config(materialized='table') }}

select
     spine.venue_id
   , coalesce ( thebrag.thebrag_venue_name, bandcamp.bandcamp_venue_name )                                          as venue_name
   , geocode.lat
   , geocode.lon
   , geocode.location_geog
   , coalesce ( geocode.geocode_location_address, thebrag.thebrag_venue_address, bandcamp.bandcamp_venue_location ) as venue_address
   , thebrag.venue_city
from
  {{ ref('stg_venue_spine') }} spine
  left join
  {{ ref('geocode_location') }} geocode
    using ( geocode_location_id )
  left join
  {{ ref('thebrag_venue') }} thebrag
    using ( thebrag_venue_id )
  left join
  {{ ref('bandcamp_venue') }} bandcamp
    using ( bandcamp_venue_id )

{{ config(materialized='table') }}

select
       spine.venue_id
     , thebrag.thebrag_venue_name as venue_name
     , geocode_results.lat
     , geocode_results.lon
from
  {{ ref('stg_venue_spine') }} spine
  join
  {{ ref('geocode_results') }} geocode_results
    using ( geocode_results_sk )
  join
  {{ ref('thebrag_venue') }} thebrag
    using ( thebrag_venue_id )

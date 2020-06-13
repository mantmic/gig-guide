{{ config(materialized='view') }}

select
    geocode_results.geocode_results_sk as venue_id
  , geocode_results.geocode_results_sk
  , venue.thebrag_venue_id
from
  {{ ref('geocode_results') }} geocode_results
  left outer join
  {{ ref('thebrag_venue') }} venue
    on geocode_results.geocode_input_address = venue.thebrag_venue_address

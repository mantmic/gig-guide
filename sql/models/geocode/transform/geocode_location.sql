{{ config(materialized='table') }}

select distinct
    geocode_location_id
  , lat
  , lon
  , location_geog
  , address_clean         as geocode_location_address
from
  {{ ref('geocode_results') }}

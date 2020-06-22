{{ config(materialized='table') }}
with dup_data as
( select
    geocode_location_id
  , lat
  , lon
  , location_geog
  , address_clean         as geocode_location_address
  , row_number() over ( partition by geocode_location_id order by geocode_location_id ) as dup_rn
from
  {{ ref('geocode_results') }}
)
select
    geocode_location_id
  , lat
  , lon
  , location_geog
  , geocode_location_address
from
  dup_data
where
  dup_rn = 1

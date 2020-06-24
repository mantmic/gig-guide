{{ config(materialized='view') }}

with dup_data as
( select
    to_hex ( {{ dbt_utils.surrogate_key('results.geocode_provider', 'results.input_address') }} )                 as geocode_results_sk
  , to_hex ( {{ dbt_utils.surrogate_key('results.geocode_provider', 'json_result.address') }} )                   as geocode_location_id
  , results.input_address                                                                                         as geocode_input_address
  , results.geocode_provider
  , json_result.raw.geometry.location.lat                                                                         as lat
  , json_result.raw.geometry.location.lng                                                                         as lon
  , json_result.address	                                                                                          as address_clean
  --, row_number() over ( partition by results.input_address, results.geocode_provider order by extract_ts desc )   as dup_rn
  , row_number() over ( partition by results.input_address order by case when results.geocode_provider = 'google' then 1 else 2 end, extract_ts desc )   as dup_rn
from
  {{ source('geocode', 'geocode_results') }} results
)
select
    geocode_results_sk
  , geocode_location_id
  , geocode_input_address
  , geocode_provider
  , lat
  , lon
  , ST_GEOGPOINT ( lon, lat ) as location_geog
  , address_clean
from
  dup_data
where
  dup_rn = 1

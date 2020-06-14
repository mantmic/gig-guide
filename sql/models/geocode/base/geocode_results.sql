{{ config(materialized='table') }}

with dup_data as
( select
    {{ dbt_utils.surrogate_key('results.geocode_provider', 'results.input_address') }}  as geocode_results_sk
  , results.input_address                                                               as geocode_input_address
  , results.geocode_provider
  , geojson_features.properties.lat                                                     as lat
  , geojson_features.properties.lng                                                     as lon
  , ST_GEOGPOINT ( geojson_features.properties.lng, geojson_features.properties.lat )   as location_geog
  , geojson_features.properties.address                                                 as address_clean
  , geojson_features.properties.raw.feature.attributes.Score                            as geocode_confidence_score
  , row_number() over ( partition by results.input_address, results.geocode_provider order by extract_ts desc ) as dup_rn
from
  {{ source('geocode', 'geocode_results') }} results
  cross join
  unnest ( results.geojson_result.features ) as geojson_features
)
select
    geocode_results_sk
  , geocode_input_address
  , geocode_provider
  , lat
  , lon
  , location_geog
  , address_clean
  , geocode_confidence_score
from
  dup_data
where
  dup_rn = 1

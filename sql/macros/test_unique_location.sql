{% macro test_unique_location(model, lat, lon, distance_threshold) %}

with transform as
( select
     {{ dbt_utils.surrogate_key( lat, lon ) }}  as location_id
   , ST_GEOGPOINT ( {{ lon }}, {{ lat }} )      as location_geography
from
  {{ model }}
),
validation as
( select
    t1.location_id
  , count ( * ) as n
from
  transform t1
  join
  transform t2
    on ST_Distance ( t1.location_geography, t2.location_geography ) < {{ distance_threshold }}
group by
  t1.location_id
having
  count ( * ) > 1
)
select
  count ( * ) as validation_errors
from
  validation

{% endmacro %}

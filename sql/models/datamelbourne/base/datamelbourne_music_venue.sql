with dup_data as
( select
    MD5 ( 'datamelbourne_music_venue' || property_number ) as venue_id
  , venue_name
  , lat
  , lon
  , row_number() over ( partition by property_number order by property_number ) as dup_rn
from
  {{ source('datamelbourne', 'datamelbourne_music_venue') }}
)
select
    venue_id
  , venue_name
  , lat
  , lon
from
  dup_data

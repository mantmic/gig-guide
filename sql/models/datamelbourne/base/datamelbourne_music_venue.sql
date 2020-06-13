with dup_data as
( select
    property_number as datamelbourne_venue_id
  , venue_name
  , lat
  , lon
  , row_number() over ( partition by property_number order by extract_ts desc ) as dup_rn
from
  {{ source('datamelbourne', 'datamelbourne_music_venue') }}
)
select
    datamelbourne_venue_id
  , venue_name
  , lat
  , lon
from
  dup_data
where
  dup_rn = 1

{{ config(materialized='table') }}

select
    venue_id
  , venue_name
  , lat
  , lon
from
  {{ ref('datamelbourne_music_venue') }}

select
  *
from
  {{ source('datamelbourne', 'datamelbourne_music_venue') }}

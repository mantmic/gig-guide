{{ config(materialized='table') }}

select distinct
    track_id                                                            as unearthed_track_id
  , details.unearthed_artist_details_sk                                 as unearthed_artist_id
  , 'https://www.triplejunearthed.com/jukebox/play/track/' || track_id  as unearthed_track_url
from
  {{ ref('unearthed_artist_details') }} details
  cross join
  unnest ( details.unearthed_track_ids ) track_id
where
  track_id is not null

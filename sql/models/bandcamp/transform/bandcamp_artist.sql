{{ config(materialized='table') }}
with dup_data as
( select
    bandcamp_artist_id
  , bandcamp_artist_name
  , bandcamp_url
  , bandcamp_artist_location
  , bandcamp_artist_bio
  , row_number() over ( partition by bandcamp_artist_id order by extract_ts desc ) as dup_rn
from
  {{ ref('bandcamp_artist_details_combined') }}
)
select
    bandcamp_artist_id
  , bandcamp_artist_name
  , bandcamp_url
  , bandcamp_artist_location
  , bandcamp_artist_bio
from
  dup_data
where
  dup_rn = 1

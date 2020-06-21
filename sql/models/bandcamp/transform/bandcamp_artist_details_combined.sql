{{ config(materialized='table') }}

with distinct_search_results as
( select distinct
    bandcamp_artist_id
  , bandcamp_artist_name
  , bandcamp_url
from
  {{ ref('bandcamp_artist_search') }}
)
select
    search.*
  , album.*
from
  distinct_search_results search
  join
  {{ ref('bandcamp_artist_albums') }} albums
    using ( bandcamp_artist_id )
  join
  {{ ref('bandcamp_album_details') }} album
    using ( bandcamp_album_id )

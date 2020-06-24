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
  , album.bandcamp_album_id
  , album.bandcamp_album_url
  , album.bandcamp_embedded_player_link
  , album.bandcamp_artist_location
  , replace ( album.bandcamp_artist_bio, '...more', '' ) as bandcamp_artist_bio
  , album.band_links
  , album.band_showography
  , album.extract_ts
from
  distinct_search_results search
  join
  {{ ref('bandcamp_artist_albums') }} albums
    using ( bandcamp_artist_id )
  join
  {{ ref('bandcamp_album_details') }} album
    using ( bandcamp_album_id )

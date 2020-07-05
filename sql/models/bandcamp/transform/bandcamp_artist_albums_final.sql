{{ config(materialized='table') }}

select
    bandcamp_artist_id
  , bandcamp_album_id
  , bandcamp_album_url
  , bandcamp_embedded_player_link
from
  {{ ref('bandcamp_artist_details_combined_final') }}

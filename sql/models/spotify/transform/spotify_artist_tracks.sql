{{ config(materialized='table') }}

select distinct
    spotify_artist_id 
  , tracks.name         as track_name 
  , tracks.preview_url  as track_url 
from 
  {{ ref('spotify_artist_top_tracks') }} artist
  cross join
  unnest ( artist.top_tracks.tracks ) tracks  
where
  tracks.preview_url is not null
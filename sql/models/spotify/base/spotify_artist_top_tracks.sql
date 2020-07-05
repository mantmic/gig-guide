{{ config(materialized='table') }}

with dup_data as 
( select 
    input_artist_id as spotify_artist_id 
  , top_tracks 
  , row_number() over ( partition by input_artist_id order by extract_ts desc ) as dup_rn 
from 
  {{ source('spotify', 'spotify_artist_top_tracks') }}
) 
select 
      spotify_artist_id
    , top_tracks 
from 
    dup_data
where 
    dup_rn = 1
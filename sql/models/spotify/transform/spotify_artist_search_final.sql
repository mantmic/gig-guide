{{ config(materialized='view') }}

with dup_data as 
( select
    search.spotify_artist_search_sk
  , search.input_search_artist
  , results.name    as artist_name 
  , results.id      as spotify_artist_id
  , results.genres  as artist_genres
  , row_number() over ( partition by spotify_artist_search_sk order by case when search.input_search_artist = results.name then 1 when lower ( search.input_search_artist ) = ( results.name ) then 2 else 3 end ) as dup_rn
from 
  {{ ref('spotify_artist_search') }} search
  cross join
  unnest ( items ) as results
) 
select 
    spotify_artist_search_sk
  , input_search_artist
  , artist_name 
  , spotify_artist_id
  , artist_genres
from 
    dup_data 
where
    dup_rn = 1
{{ config(materialized='view') }}

with dup_data as 
( select
    results.name    as artist_name 
  , results.id      as spotify_artist_id
  , results.genres  as artist_genres
  , row_number() over ( partition by results.name order by results.name ) as dup_rn
from 
  {{ ref('spotify_artist_search') }} search
  cross join
  unnest ( items ) as results
) 
select 
    artist_name 
  , spotify_artist_id
  , artist_genres
from 
    dup_data 
where
    dup_rn = 1
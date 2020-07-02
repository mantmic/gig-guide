{{ config(materialized='view') }}

select
    artist.google_search_artist_id 
  , other_info.value                as artist_genre
from
  {{ ref('google_search_artist_details') }} artist 
  cross join
  unnest ( other_info ) other_info
where
  other_info.description = 'genres'
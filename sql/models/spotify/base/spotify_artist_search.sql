{{ config(materialized='table') }}

with dup_data as 
( select
    to_hex ( {{ dbt_utils.surrogate_key('search.input_search_artist') }} )  as spotify_artist_search_sk
  , search.input_search_artist
  , search.search_results.artists.items as items 
  , row_number() over ( partition by search.input_search_artist order by extract_ts desc ) as dup_rn
from 
  {{ source('spotify', 'spotify_artist_search') }} search
) 
select 
      spotify_artist_search_sk
    , input_search_artist
    , items
from
    dup_data 
where 
    dup_rn = 1

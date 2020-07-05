{{ config(materialized='table') }}

with dup_data as 
( select
    coalesce ( combined_bandcamp_artist_id, artist_details.bandcamp_artist_id ) as bandcamp_artist_id
  , artist_details.bandcamp_artist_name
  , artist_details.bandcamp_url
  , artist_details.bandcamp_album_id
  , artist_details.bandcamp_album_url
  , artist_details.bandcamp_embedded_player_link
  , artist_details.bandcamp_artist_location
  , artist_details.bandcamp_artist_bio
  , artist_details.band_links
  , artist_details.band_showography
  , artist_details.extract_ts
  , row_number() over ( partition by coalesce ( combined_bandcamp_artist_id, artist_details.bandcamp_artist_id ) order by artist_details.bandcamp_url desc ) as dup_rn 
from
  {{ ref('bandcamp_artist_details_combined') }} artist_details
  left join
  {{ ref('bandcamp_artist_multi_profile') }} multi_profile 
    using ( bandcamp_artist_id )
) 
select  
  *
from 
  dup_data
where
  dup_rn = 1
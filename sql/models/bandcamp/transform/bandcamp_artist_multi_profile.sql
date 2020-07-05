{{ config(materialized='view') }}

-- cte of artists with the same name who have played gigs together 
with gig_artist as 
( select
    gig.bandcamp_gig_id
  , artist.bandcamp_artist_name 
  , array_agg ( distinct artist.bandcamp_artist_id ) as bandcamp_artist_id_array 
from
  {{ ref('bandcamp_gig') }} gig
  join
  {{ ref('bandcamp_artist_details_combined') }} artist 
    using ( bandcamp_artist_id ) 
group by 
    gig.bandcamp_gig_id
  , artist.bandcamp_artist_name  
having 
  count ( distinct artist.bandcamp_artist_id ) > 1
),
combined_data as 
( select    
    array_to_string ( bandcamp_artist_id_array, '_' ) as combined_bandcamp_artist_id
  , bandcamp_artist_id
from
  gig_artist 
  cross join 
  unnest ( gig_artist.bandcamp_artist_id_array ) as bandcamp_artist_id
) 
select
      to_hex ( {{ dbt_utils.surrogate_key('combined_bandcamp_artist_id') }} ) as combined_bandcamp_artist_id
    , bandcamp_artist_id
from 
    combined_data
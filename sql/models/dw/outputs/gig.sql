{{ config(materialized='view') }}

with gig_artist as (
select
    gig_artist.gig_id 
  , array_agg ( struct ( 
      artist.artist_id 
    , artist.artist_name 
    , artist.artist_website_url 
    , artist.artist_social
    , artist.artist_music
    ) ) as artists
from 
  dev.dim_gig_artist gig_artist
  join
  dev.artist artist 
    using ( artist_id ) 
group by 
  gig_artist.gig_id
)
select
    gig.gig_id
  , cast ( gig.gig_date as string ) as gig_date 
  , gig.gig_url
  , gig.gig_price
  , gig_artist.artists
  , struct ( 
          venue.venue_id
        , venue.venue_name
        , venue.lat
        , venue.lon
    )                       as venue 
from 
  {{ ref('dim_gig') }} gig 
  left join
  gig_artist 
    using ( gig_id ) 
  left join
  {{ ref('venue') }} venue  
    using ( venue_id ) 
where
  gig.gig_date between current_date('Australia/Melbourne') and date_add ( current_date('Australia/Melbourne'), interval 7 day ) 
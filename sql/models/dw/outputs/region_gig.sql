{{ config(materialized='view') }}

with gigs as 
( select 
    region.region_name 
  , venue.venue_name 
  , gig.gig_date
  , gig.gig_datetime
  , format_datetime ( '%r', gig.gig_datetime )  as gig_time 
  , format_date (  '%A', gig.gig_date )         as day_of_week
  , gig.gig_url 
  , array_agg ( struct ( artist.artist_name, artist.artist_website_url, artist_music.artist_music_source, artist_music.artist_music_url ) ) as gig_artists
from 
  {{ ref('dim_venue') }} venue 
  join
  {{ ref('mapping_social_post_region') }} region 
    on ST_CONTAINS ( ST_GEOGFROMTEXT ( region.region_wkt ), venue.location_geog )
  join
  {{ ref('dim_gig') }} gig
    using ( venue_id ) 
  join
  {{ ref('dim_gig_artist') }} gig_artist 
    using ( gig_id ) 
  join
  {{ ref('dim_artist') }} artist 
    using ( artist_id ) 
  left join
  {{ ref('dim_artist_music') }} artist_music
    on artist.artist_id = artist_music.artist_id
    and artist_music.artist_music_rank = 1
where
  gig.gig_date between date_trunc ( current_date ( 'Australia/Sydney' ), week (SUNDAY) ) and date_add ( date_trunc ( current_date ( 'Australia/Sydney' ), week (SUNDAY) ), interval 7 day )
  and gig.gig_cancelled_yn != 'Y'
group by 
    region.region_name 
  , venue.venue_name 
  , gig.gig_date
  , format_date (  '%A', gig.gig_date )
  , gig.gig_url 
  , gig.gig_datetime
  , format_datetime ( '%r', gig.gig_datetime )
) 
select  
    region_name
  , array_agg ( 
      struct (
          venue_name 
        , gig_date
        , gig_time
        , day_of_week
        , gig_url 
        , gig_artists
      )
    order by 
        gig_date
      , venue_name
      , gig_datetime
  ) as region_gigs
from 
  gigs 
group by 
  region_name 
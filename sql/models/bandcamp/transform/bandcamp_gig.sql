{{ config(materialized='table') }}

with dup_data as
( select
    artist_albums.bandcamp_artist_id
  , bandcamp_gigs.show_location_full  as gig_location
  , bandcamp_gigs.show_venue          as gig_venue_name
  , bandcamp_gigs.show_url            as gig_url
  , parse_date ( '%b %d %E4Y', bandcamp_gigs.show_date || ' ' || extract ( year from album_details.extract_ts ) ) as gig_date
  , cast ( album_details.extract_ts as date ) as extract_date
from
  {{ ref('bandcamp_album_details') }} album_details
  join
  {{ ref('bandcamp_artist_albums') }} artist_albums
    using ( bandcamp_album_id )
  cross join
  unnest ( album_details.band_showography ) bandcamp_gigs
)
select distinct
    bandcamp_artist_id
  , gig_location
  , gig_venue_name
  , gig_url
  , case when gig_date > extract_date then gig_date
         else date_add ( gig_date, interval 1 year )
    end as gig_date
from
  dup_data

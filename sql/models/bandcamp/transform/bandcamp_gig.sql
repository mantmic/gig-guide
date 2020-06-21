{{ config(materialized='table') }}

with dup_data as
( select
    artist_albums.bandcamp_artist_id
  , bandcamp_gigs.show_location_full                                      as bandcamp_venue_location
  , bandcamp_gigs.show_venue                                              as bandcamp_venue_name
  , to_hex ( {{ dbt_utils.surrogate_key('bandcamp_gigs.show_venue') }} )  as bandcamp_venue_id
  , bandcamp_gigs.show_url                                                as gig_url
  , parse_date ( '%b %d %E4Y', bandcamp_gigs.show_date || ' ' || extract ( year from album_details.extract_ts ) ) as gig_date
  , cast ( album_details.extract_ts as date ) as extract_date
from
  {{ ref('bandcamp_album_details') }} album_details
  join
  {{ ref('bandcamp_artist_albums') }} artist_albums
    using ( bandcamp_album_id )
  cross join
  unnest ( album_details.band_showography ) bandcamp_gigs
),
-- gig dates only have day and month - assuming that the gig date is the next occurance of that date after the extract_ts
gig_date_correction as
( select distinct
    bandcamp_artist_id
  , bandcamp_venue_location
  , bandcamp_venue_name
  , bandcamp_venue_id
  , gig_url
  , case when gig_date > extract_date then gig_date
         else date_add ( gig_date, interval 1 year )
    end as gig_date
from
  dup_data
)
select
    to_hex ( {{ dbt_utils.surrogate_key('bandcamp_venue_id','gig_date') }} )  as bandcamp_gig_id
  , bandcamp_artist_id
  , bandcamp_venue_location
  , bandcamp_venue_name
  , bandcamp_venue_id
  , gig_url
  , gig_date
from
  gig_date_correction

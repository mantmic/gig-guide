{{ config(materialized='view') }}

with dup_data as 
( select
    to_hex ( {{ dbt_utils.surrogate_key('venue_shows.show_reverbnation_url') }} )   as reverbnation_gig_id 
  , venue.reverbnation_venue_id
  , venue_shows.show_reverbnation_url                                               as reverbnation_url
  , {{ clean_url('venue_shows.show_ticket_url') }}                                  as ticket_url
  , datetime ( venue_shows.show_datetime, 'Australia/Melbourne' )                   as gig_datetime
  , cast ( datetime (venue_shows.show_datetime, 'Australia/Melbourne' ) as date )   as gig_date
  , venue_shows.show_artists
  , row_number() over ( partition by venue_shows.show_reverbnation_url order by venue.extract_ts desc ) as dup_rn
from 
  {{ ref('reverbnation_venue_shows') }} venue
  cross join 
  unnest ( venue.shows ) venue_shows 
) 
select
  *
from 
  dup_data
where
  dup_rn = 1
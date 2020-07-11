{{ config(materialized='view') }}

with dup_data as 
( select 
    to_hex ( {{ dbt_utils.surrogate_key('oztix_ticket_url') }} )                        as oztix_gig_details_sk
  , oztix_ticket_url
  , trim ( event_location )                                                             as event_location 
  , trim ( event_venue_name )                                                           as event_venue_name
  , event_tickets 
  , trim ( event_name )                                                                 as event_name
  , parse_datetime ( '%d %B %E4Y %I:%M %p', split(event_datetime, ', ' )[ordinal(2)] )  as event_datetime
  , row_number() over ( partition by oztix_ticket_url order by extract_ts desc )        as dup_rn
from 
  {{ source('oztix', 'oztix_gig_details') }}
) 
select 
    *
from 
    dup_data
where 
    dup_rn = 1
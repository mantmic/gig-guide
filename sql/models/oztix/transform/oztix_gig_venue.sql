{{ config(materialized='view') }}

with clean as 
( select 
    oztix_gig_details_sk
  , coalesce ( trim ( event_venue_name ), trim ( split ( event_location, '(' )[ordinal(1)] ) )  as venue_name
  , trim ( event_location )                                                                     as venue_location
from 
  {{ ref('oztix_gig_details') }} gig
) 
select
    oztix_gig_details_sk
  , to_hex ( {{ dbt_utils.surrogate_key('venue_name') }} ) as oztix_venue_id 
  , venue_name
  , venue_location
from
  clean
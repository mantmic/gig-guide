{{ config(materialized='view') }}

select distinct 
    oztix_venue_id 
  , venue_name
  , venue_location
from
  {{ ref('oztix_gig_venue') }}
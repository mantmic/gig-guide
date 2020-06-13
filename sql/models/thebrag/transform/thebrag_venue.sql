{{ config(materialized='table') }}

select distinct
    gigs.thebrag_venue_id
  , gigs.gig_city                       as venue_city
  , gig_details.thebrag_venue_address
  , gigs.thebrag_venue_name
from
  {{ ref('thebrag_gig_details') }} gig_details
  join
  {{ ref('thebrag_gigs') }} gigs
    using ( thebrag_gig_id )

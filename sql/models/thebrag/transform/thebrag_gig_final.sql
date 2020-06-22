{{ config(materialized='table') }}

select distinct 
    gigs.thebrag_gig_id
  , gigs.thebrag_gig_url
  , gigs.thebrag_venue_id
  , gigs.gig_date
  , gig_details.gig_ticket_url
from
  {{ ref('thebrag_gigs') }} gigs
  join
  {{ ref('thebrag_gig_details') }} gig_details
    using ( thebrag_gig_id )

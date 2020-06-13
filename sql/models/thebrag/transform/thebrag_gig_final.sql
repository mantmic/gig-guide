{{ config(materialized='table') }}
select
    gigs.thebrag_gig_id
  , gigs.thebrag_gig_url
  , gigs.thebrag_venue_id
  , gigs.gig_date
from
  {{ ref('thebrag_gigs') }} gigs
  join
  {{ ref('thebrag_gig_details') }} gig_details
    using ( thebrag_gig_id )

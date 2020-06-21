{{ config(materialized='table') }}

--get gigs from thebrag
with thebrag as
( select
    thebrag.thebrag_gig_id
  , thebrag.gig_date
  , thebrag_venue.venue_id
from
  {{ ref('thebrag_gig_final') }} thebrag
  join
  {{ ref('stg_venue_spine') }} thebrag_venue
    using ( thebrag_venue_id )
),
-- get gigs from bandcamp
bandcamp as
( select
    bandcamp.bandcamp_gig_id
  , bandcamp.gig_date
  , bandcamp_venue.venue_id
from
  {{ ref('bandcamp_gig') }} bandcamp
  join
  {{ ref('stg_venue_spine') }} bandcamp_venue
    using ( bandcamp_venue_id )
)
select
    to_hex ( {{ dbt_utils.surrogate_key('thebrag_gig_id','bandcamp_gig_id') }} )  as gig_id
  , bandcamp.bandcamp_gig_id
  , thebrag.thebrag_gig_id
from
  bandcamp
  full outer join
  thebrag
    on bandcamp.gig_date  = thebrag.gig_date
    and bandcamp.venue_id = thebrag.venue_id

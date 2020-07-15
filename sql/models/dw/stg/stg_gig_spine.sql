{{ config(materialized='table') }}

--get gigs from thebrag
with thebrag as
( select
    to_hex ( {{ dbt_utils.surrogate_key('venue_id','gig_date') }} ) as gig_spine_sk
  , thebrag.thebrag_gig_id
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
    to_hex ( {{ dbt_utils.surrogate_key('venue_id','gig_date') }} ) as gig_spine_sk
  , bandcamp.bandcamp_gig_id
  , bandcamp.gig_date
  , bandcamp_venue.venue_id
from
  {{ ref('bandcamp_gig_final') }} bandcamp
  join
  {{ ref('stg_venue_spine') }} bandcamp_venue
    using ( bandcamp_venue_id )
),
-- get gigs from reverbnation
reverbnation as 
( select
    to_hex ( {{ dbt_utils.surrogate_key('venue_id','gig_date') }} ) as gig_spine_sk
  , reverbnation.reverbnation_gig_id
  , reverbnation.gig_date
  , reverbnation_venue.venue_id
from
  {{ ref('reverbnation_gig') }} reverbnation
  join
  {{ ref('stg_venue_spine') }} reverbnation_venue
    using ( reverbnation_venue_id )
)
select
    to_hex ( {{ dbt_utils.surrogate_key('thebrag_gig_id','bandcamp_gig_id','reverbnation_gig_id' ) }} )  as gig_id
  , bandcamp.bandcamp_gig_id
  , thebrag.thebrag_gig_id
  , reverbnation.reverbnation_gig_id
  , coalesce ( bandcamp.venue_id, thebrag.venue_id, reverbnation.venue_id )                              as venue_id
from
  bandcamp
  full outer join
  thebrag
    using (gig_spine_sk )
  full outer join
  reverbnation
    using ( gig_spine_sk )

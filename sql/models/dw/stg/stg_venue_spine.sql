{{ config(materialized='table') }}

-- get geocode_location_ids for venues
with thebrag_geocoded as
( select
    geocode_results.geocode_location_id
  , venue.thebrag_venue_id
from
  {{ ref('thebrag_venue') }} venue
  left outer join
  {{ ref('geocode_results') }} geocode_results
    on geocode_results.geocode_input_address = venue.thebrag_venue_address
),
bandcamp_geocoded as
( select
    bandcamp.bandcamp_venue_id
  , geocode.geocode_location_id
from
  {{ ref('bandcamp_venue') }} bandcamp
  left outer join
  {{ ref('geocode_results') }} geocode
    on bandcamp.bandcamp_venue_location = geocode.geocode_input_address
),
reverbnation_geocoded as 
( select
    reverbnation.reverbnation_venue_id
  , geocode.geocode_location_id
from
  {{ ref('reverbnation_venue') }} reverbnation
  left outer join
  {{ ref('geocode_results') }} geocode
    on reverbnation.venue_location_full = geocode.geocode_input_address
), 
results_combined as 
( select
    thebrag_geocoded.thebrag_venue_id
  , bandcamp_geocoded.bandcamp_venue_id
  , reverbnation_geocoded.reverbnation_venue_id
  , coalesce ( thebrag_geocoded.geocode_location_id, bandcamp_geocoded.geocode_location_id, reverbnation_geocoded.geocode_location_id ) as geocode_location_id
from
  thebrag_geocoded
  full outer join
  bandcamp_geocoded
    using ( geocode_location_id )
  full outer join
  reverbnation_geocoded
    using ( geocode_location_id )
)
select
    to_hex ( {{ dbt_utils.surrogate_key('geocode_location_id', 'thebrag_venue_id', 'bandcamp_venue_id', 'reverbnation_venue_id') }} ) as venue_id
  , *
from
  results_combined

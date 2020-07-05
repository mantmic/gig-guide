{{ config(materialized='view') }}

-- handle two artists playing at the same gig
with de_dup as 
( select distinct 
    gig.bandcamp_gig_id
  , coalesce ( multi_profile.combined_bandcamp_artist_id, gig.bandcamp_artist_id ) as bandcamp_artist_id 
  , gig.bandcamp_venue_id
  , gig.gig_url
  , gig.gig_date
from
  {{ ref('bandcamp_gig') }} gig 
  left join
  {{ ref('bandcamp_artist_multi_profile') }} multi_profile 
    using ( bandcamp_artist_id )
) 
select  
    bandcamp_gig_id
  , bandcamp_venue_id
  , gig_url
  , gig_date
  , array_agg ( bandcamp_artist_id ) as bandcamp_artists
from 
  de_dup
group by 
    bandcamp_gig_id
  , bandcamp_venue_id
  , gig_url
  , gig_date
{{ config(materialized='table') }}

with gig_artist as 
( select
    gig_spine.gig_id
  , artist_spine.artist_id
from
  {{ ref('thebrag_gig_artist') }} thebrag
  join
  {{ ref('stg_gig_spine') }} gig_spine
    using ( thebrag_gig_id )
  join
  {{ ref('stg_artist_spine') }} artist_spine
    using ( thebrag_artist_id )
union distinct
select
    gig_spine.gig_id
  , artist_spine.artist_id
from
  {{ ref('bandcamp_gig_final') }} bandcamp
  cross join
  unnest ( bandcamp.bandcamp_artists ) bandcamp_artist_id 
  join
  {{ ref('stg_gig_spine') }} gig_spine
    using ( bandcamp_gig_id )
  join
  {{ ref('stg_artist_spine') }} artist_spine
    using ( bandcamp_artist_id )
) 
select  
    to_hex ( {{ dbt_utils.surrogate_key('gig_id','artist_id' ) }} ) as gig_artist_sk
  , *
from  
  gig_artist
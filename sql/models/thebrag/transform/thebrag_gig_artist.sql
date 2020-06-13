{{ config(materialized='table') }}

with artist_clean as
( select distinct
      gig_details.thebrag_gig_id
    , trim ( gig_artist )         as gig_artist
from
  {{ ref('thebrag_gig_details') }} gig_details
  cross join
  unnest ( gig_details.thebrag_gig_artist_array ) as gig_artist
where
  gig_artist is not null
)
select
  *
from
  artist_clean
where
  gig_artist not in ( '' )

{{ config(materialized='view') }}

with artist_clean as
( select distinct
      gig_details.thebrag_gig_id
    , trim ( thebrag_artist_name )   as thebrag_artist_name
from
  {{ ref('thebrag_gig_details') }} gig_details
  cross join
  unnest ( gig_details.thebrag_gig_artist_array ) as thebrag_artist_name
where
  thebrag_artist_name is not null
)
select
    thebrag_gig_id
  , thebrag_artist_name
  , to_hex ( {{ dbt_utils.surrogate_key('thebrag_artist_name') }} ) as thebrag_artist_id
from
  artist_clean
where
  thebrag_artist_name not in ( '' )

{{ config(materialized='view') }}

with dup_data as
( select
    to_hex ( {{ dbt_utils.surrogate_key('gig_url') }} )                 as thebrag_gig_id
  , gig_url                                                             as thebrag_gig_url
  , cast ( gig_date as date )                                           as gig_date
  , gig_city
  , trim ( gig_location )                                               as thebrag_venue_name
  , row_number() over ( partition by gig_url order by extract_ts desc ) as dup_rn
from
  {{ source('thebrag', 'thebrag_gigs') }}
)
select
    thebrag_gig_id
  , to_hex ( {{ dbt_utils.surrogate_key('thebrag_venue_name') }} )  as thebrag_venue_id
  , thebrag_venue_name
  , thebrag_gig_url
  , gig_date
  , gig_city
from
  dup_data
where
  dup_rn = 1

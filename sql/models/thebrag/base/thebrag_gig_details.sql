{{ config(materialized='view') }}

with dup_data as
( select
     md5 ( gig_url )                              as thebrag_gig_id
   , gig_ticket_url
   , gig_location_address                         as thebrag_venue_address
   , SPLIT ( gig_artist, "_")                     as thebrag_gig_artist_array
   , row_number() over ( partition by gig_url order by extract_ts desc ) as dup_rn
from
  {{ source('thebrag', 'thebrag_gig_details') }}
)
select
     thebrag_gig_id
   , gig_ticket_url
   , thebrag_venue_address
   , thebrag_gig_artist_array
from
  dup_data
where
  dup_rn = 1

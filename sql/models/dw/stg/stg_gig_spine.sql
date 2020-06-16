{{ config(materialized='table') }}

select
    to_hex ( {{ dbt_utils.surrogate_key('thebrag.thebrag_gig_id') }} )  as gig_id
  , thebrag.thebrag_gig_id
  , thebrag.gig_date
  , thebrag_venue.venue_id
  , moshtix.ticket_price                                                as gig_price
from
  {{ ref('thebrag_gig_final') }} thebrag
  left outer join
  {{ ref('stg_venue_spine') }} thebrag_venue
    using ( thebrag_venue_id )
  left outer join
  {{ ref('moshtix_gig_details_final') }} moshtix
    on thebrag.gig_ticket_url = moshtix.moshtix_url

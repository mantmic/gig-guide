{{ config(materialized='table') }}

select
    spine.gig_id
  , thebrag.gig_date
  , spine.venue_id
  , moshtix.ticket_price as gig_price
from
  {{ ref('stg_gig_spine') }} spine
  left join
  {{ ref('thebrag_gig_final') }} thebrag
    using ( thebrag_gig_id )
  left outer join
  {{ ref('moshtix_gig_details_final') }} moshtix
    on thebrag.gig_ticket_url = moshtix.moshtix_url

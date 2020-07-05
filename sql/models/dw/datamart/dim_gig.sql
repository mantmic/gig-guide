{{ config(materialized='table') }}

select
    spine.gig_id
  , coalesce ( thebrag.gig_date, bandcamp.gig_date )      as gig_date
  , spine.venue_id
  , moshtix.ticket_price                                  as gig_price
  , coalesce ( thebrag.gig_ticket_url, bandcamp.gig_url ) as gig_url
from
  {{ ref('stg_gig_spine') }} spine
  left join
  {{ ref('thebrag_gig_final') }} thebrag
    using ( thebrag_gig_id )
  left join
  {{ ref('moshtix_gig_details_final') }} moshtix
    on thebrag.gig_ticket_url = moshtix.moshtix_url
  left join
  {{ ref('bandcamp_gig_final') }} bandcamp
    using ( bandcamp_gig_id )

{{ config(materialized='table') }}

select
    spine.gig_id
  , coalesce ( thebrag.gig_date, bandcamp.gig_date )      as gig_date
  , coalesce ( moshtix.gig_datetime, oztix.gig_datetime ) as gig_datetime
  , spine.venue_id
  , coalesce ( moshtix.ticket_price, oztix.gig_price )    as gig_price
  , coalesce ( thebrag.gig_ticket_url, bandcamp.gig_url ) as gig_url
  , coalesce ( oztix.tickets_available_yn, 'U' )          as tickets_available_yn
  , coalesce ( oztix.gig_cancelled_yn, 'U' )              as gig_cancelled_yn
from
  {{ ref('stg_gig_spine') }} spine
  left join
  {{ ref('thebrag_gig_final') }} thebrag
    using ( thebrag_gig_id )
  left join
  {{ ref('moshtix_gig_details_final') }} moshtix
    on thebrag.gig_ticket_url = moshtix.moshtix_url
  left join
  {{ ref('oztix_gig') }} oztix
    on thebrag.gig_ticket_url = oztix.oztix_ticket_url
  left join
  {{ ref('bandcamp_gig_final') }} bandcamp
    using ( bandcamp_gig_id )

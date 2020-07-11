{{ config(materialized='table') }}

select
    moshtix_url
  , gig_datetime
  , safe_cast ( replace ( min ( ticket_types.ticket_price ), '$', '' ) as numeric ) as ticket_price
from
  {{ ref('moshtix_gig_details') }} gig_details
  cross join
  unnest ( moshtix_ticket_types ) as ticket_types
group by
    moshtix_url
  , gig_datetime

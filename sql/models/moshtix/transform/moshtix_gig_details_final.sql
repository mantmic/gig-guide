{{ config(materialized='view') }}

select
    moshtix_url
  , gig_datetime
  , min ( ticket_types.ticket_price ) as ticket_price
from
  {{ ref('moshtix_gig_details') }} gig_details
  cross join
  unnest ( moshtix_ticket_types ) as ticket_types
group by
    moshtix_url
  , gig_datetime

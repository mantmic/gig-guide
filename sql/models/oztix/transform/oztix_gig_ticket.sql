{{ config(materialized='view') }}

select 
    gig.oztix_gig_details_sk
  , trim ( event_tickets.ticket_name )                                                                      as ticket_name
  , safe_cast ( replace ( event_tickets.ticket_price,'$','' ) as numeric )                                  as ticket_price
  , trim ( event_tickets.ticket_description )                                                               as ticket_description
  , case when event_tickets.ticket_description like 'cancelled' then 'Y'
         else 'N'
    end                                                                                                     as gig_cancelled_yn
  , row_number() over ( partition by gig.oztix_gig_details_sk order by trim ( event_tickets.ticket_name ) ) as ticket_order
from 
  {{ ref('oztix_gig_details') }} gig 
  cross join
  unnest ( gig.event_tickets ) event_tickets 
  
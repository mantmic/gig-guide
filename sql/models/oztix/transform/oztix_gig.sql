{{ config(materialized='table') }}

select 
    gig.oztix_gig_details_sk                      as oztix_gig_id 
  , gig.oztix_ticket_url
  , gig_venue.oztix_venue_id 
  , ticket.gig_cancelled_yn 
  , case when ticket.ticket_name is null then 'N'
         else 'Y'
    end as tickets_available_yn 
  , ticket.ticket_price                           as gig_price
  , cast ( gig.event_datetime as date )           as gig_date
  , gig.event_datetime                            as gig_datetime
from 
  {{ ref('oztix_gig_details') }} gig
  left join
  {{ ref('oztix_gig_venue') }} gig_venue
    using ( oztix_gig_details_sk ) 
  left join
  {{ ref('oztix_gig_ticket') }} ticket 
    on gig.oztix_gig_details_sk = ticket.oztix_gig_details_sk
    and ticket.ticket_order = 1
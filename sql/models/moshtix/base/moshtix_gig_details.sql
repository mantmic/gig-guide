{{ config(materialized='view') }}

with dup_data as
( select
    url                                                               as moshtix_url
  , ticket_types                                                      as moshtix_ticket_types
  , parse_datetime ( '%I:%M%p, %a %d %B, %Y',gig_datetime )           as gig_datetime
  , row_number() over ( partition by url order by extract_ts desc )   as dup_rn
from
  {{ source('moshtix', 'moshtix_gig_details') }}
)
select
    moshtix_url
  , moshtix_ticket_types
  , gig_datetime
from
  dup_data
where
  dup_rn = 1

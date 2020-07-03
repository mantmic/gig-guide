{{ config(materialized='view') }}

select
    gig.gig_id
  , gig.gig_date
  , gig.gig_url
from 
  {{ ref('dim_gig') }} gig 
where
  gig.gig_date between current_date('Australia/Melbourne') and date_add ( current_date('Australia/Melbourne'), interval 7 day ) 
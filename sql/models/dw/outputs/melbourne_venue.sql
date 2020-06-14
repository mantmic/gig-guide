{{ config(materialized='view') }}

select
     venue_id
   , venue_name
   , lat
   , lon
from
  {{ ref('dim_venue') }}
where
  venue_city = 'melbourne'

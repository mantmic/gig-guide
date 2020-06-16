{{ config(materialized='table') }}

select
    spine.gig_id
  , spine.gig_date
  , spine.venue_id
  , spine.gig_price
from
  {{ ref('stg_gig_spine') }} spine

{{ config(materialized='view') }}

select 
    venue.reverbnation_venue_id
  , social_links.social_link_type 
  , social_links.social_link_url 
from 
  {{ ref('reverbnation_venue_shows') }} venue 
  cross join
  unnest ( venue.venue_social_links ) social_links
where
  dup_rn = 1
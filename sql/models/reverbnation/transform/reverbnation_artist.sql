{{ config(materialized='view') }}

with dup_data as 
( select
    reverbnation_artist_id
  , artist_reverbnation_url as reverbnation_url
  , artist_image_url 
  , artist_name
  , row_number() over ( partition by reverbnation_artist_id order by gig_datetime desc ) as dup_rn
from 
  {{ ref('reverbnation_gig_artist') }}
) 
select 
  *
from 
  dup_data 
where
  dup_rn = 1
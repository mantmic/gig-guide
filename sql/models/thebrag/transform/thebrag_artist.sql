{{ config(materialized='table') }}

select distinct
    thebrag_artist_name
  , thebrag_artist_id
from
  {{ ref('thebrag_gig_artist') }}

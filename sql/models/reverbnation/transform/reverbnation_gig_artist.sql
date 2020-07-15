{{ config(materialized='view') }}

select
    gig.reverbnation_gig_id
  , gig.gig_datetime
  , artist.artist_reverbnation_url 
  , artist.artist_image_url 
  , trim ( artist.artist_name )                                     as artist_name
  , to_hex ( {{ dbt_utils.surrogate_key('artist.artist_name') }} )  as reverbnation_artist_id 
from 
  {{ ref('reverbnation_gig') }} gig
  cross join
  unnest ( gig.show_artists ) artist 
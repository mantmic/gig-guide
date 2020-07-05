{{ config(materialized='table') }}

select distinct
     artist.bandcamp_artist_id
   , {{ clean_url('band_links.link_url') }}   as bandcamp_link_url
   , band_links.link_text                     as bandcamp_link_description
from
  {{ ref('bandcamp_artist_details_combined_final') }} artist
  cross join
  unnest ( artist.band_links ) band_links

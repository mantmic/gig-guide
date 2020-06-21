{{ config(materialized='table') }}

with artist_links as
( select distinct
     artist.bandcamp_artist_id
   , {{ clean_url('band_links.link_url') }}   as bandcamp_link_url
   , band_links.link_text                     as bandcamp_link_description
from
  {{ ref('bandcamp_artist_details_combined') }} artist
  cross join
  unnest ( artist.band_links ) band_links
)
, link_classification as
( select
      artist_links.bandcamp_link_url
    , mapping.social_media_website
 from
  artist_links
  cross join
  {{ ref('mapping_social_media_website') }} mapping
where
  artist_links.bandcamp_link_url like mapping.url_pattern
)
select
  *
from
  artist_links
  left outer join
  link_classification classification
    using ( bandcamp_link_url )

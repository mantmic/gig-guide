{{ config(materialized='table') }}

with artist_links as
( select
     spine.artist_id
   , bandcamp.bandcamp_link_url as artist_social_url
from
  {{ ref('bandcamp_artist_links') }} bandcamp
  join
  {{ ref('stg_artist_spine') }} spine
    using ( bandcamp_artist_id )
union distinct
select
     spine.artist_id
   , unearthed.social_link_url as artist_social_url
from
  {{ ref('unearthed_social_links') }} unearthed
  join
  {{ ref('stg_artist_spine') }} spine
    using ( unearthed_artist_id )
)
, link_classification as
( select
      artist_links.artist_social_url
    , mapping.social_media_website
 from
  artist_links
  cross join
  {{ ref('mapping_social_media_website') }} mapping
where
  artist_links.artist_social_url like mapping.url_pattern
)
select
  *
from
  artist_links
  left outer join
  link_classification classification
    using ( artist_social_url )

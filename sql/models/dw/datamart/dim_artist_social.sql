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
union distinct 
select
    spine.artist_id
  , google.social_media_url as artist_social_url
from
  {{ ref('stg_artist_spine') }} spine
  join
  {{ ref('google_search_artist_social_media') }} google 
    using ( google_search_artist_id )
union distinct 
-- include artist websites in case they are social media pages 
select
    artist.artist_id
  , artist.artist_website_url as artist_social_url
from 
  {{ ref('dim_artist') }} artist 
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
    artist_links.artist_id
  , artist_links.artist_social_url
  , classification.social_media_website
  , case when classification.social_media_website = 'instagram' then ARRAY_REVERSE ( SPLIT ( artist_links.artist_social_url,'/' ) )[SAFE_OFFSET(0)]
    end as social_media_handle 
from
  artist_links
  join
  link_classification classification
    using ( artist_social_url )
{{ config(materialized='table') }}
--  match on artist website to bio
select
    unearthed.unearthed_artist_id
  , google.google_search_artist_id
from
  {{ ref('unearthed_artist') }} unearthed
  join
  {{ ref('google_search_artist') }} google
    on unearthed.artist_website_url = google.bio_link_url
union distinct
-- match on artist website to social media 
select
    unearthed.unearthed_artist_id
  , google.google_search_artist_id
from
  {{ ref('unearthed_artist') }} unearthed
  join
  {{ ref('google_search_artist_social_media') }} google
    on unearthed.artist_website_url = google.social_media_url
union distinct 
-- match on artist website to music platform 
select
    unearthed.unearthed_artist_id
  , google.google_search_artist_id
from
  {{ ref('unearthed_artist') }} unearthed
  join
  {{ ref('google_search_artist_music_platform') }} google
    on unearthed.artist_website_url = google.music_platform_url
-- match unearthed social media links to google social media links 
union distinct 
select
    unearthed.unearthed_artist_id
  , google.google_search_artist_id
from
  {{ ref('unearthed_social_links') }} unearthed
  join
  {{ ref('google_search_artist_social_media') }} google
    on unearthed.social_link_url = google.social_media_url
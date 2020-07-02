{{ config(materialized='table') }}
--  match on artist website to bio = to bandcamp url 
select
      google.google_search_artist_id
    , bandcamp.bandcamp_artist_id
from
  {{ ref('google_search_artist') }} google
  join  
  {{ ref('bandcamp_artist') }} bandcamp
    on google.bio_link_url = bandcamp.bandcamp_url
union distinct
-- match on music platform being bandcamp 
select
      google.google_search_artist_id
    , bandcamp.bandcamp_artist_id
from
  {{ ref('google_search_artist_music_platform') }} google
  join  
  {{ ref('bandcamp_artist') }} bandcamp
    on google.music_platform_url = bandcamp.bandcamp_url
union distinct
-- match on bandcamp links to music platform 
select
      google.google_search_artist_id
    , bandcamp.bandcamp_artist_id
from
  {{ ref('google_search_artist_music_platform') }} google
  join
  {{ ref('bandcamp_artist_links') }} bandcamp
    on google.music_platform_url = bandcamp.bandcamp_link_url
union distinct
-- match on bandcamp links to google social media  
select
      google.google_search_artist_id
    , bandcamp.bandcamp_artist_id
from
  {{ ref('google_search_artist_social_media') }} google
  join
  {{ ref('bandcamp_artist_links') }} bandcamp
    on google.social_media_url = bandcamp.bandcamp_link_url
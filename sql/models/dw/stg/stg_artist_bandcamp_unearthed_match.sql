{{ config(materialized='table') }}
-- this table matches artists across bandcamp and unearthed using artist links on either website
select
    unearthed.unearthed_artist_id
  , bandcamp.bandcamp_artist_id
from
  {{ ref('unearthed_artist') }} unearthed
  join
  {{ ref('bandcamp_artist') }} bandcamp
    on unearthed.artist_website_url = bandcamp.bandcamp_url
union distinct
select
    unearthed.unearthed_artist_id
  , artist_links.bandcamp_artist_id
from
  {{ ref('unearthed_artist') }} unearthed
  join
  {{ ref('bandcamp_artist_links') }} artist_links
    on unearthed.artist_website_url = artist_links.bandcamp_link_url

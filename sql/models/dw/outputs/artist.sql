{{ config(materialized='view') }}

with music_ranked as 
( select 
    artist_id
  , case when artist_music_source = 'spotify' then 'media' 
         else artist_music_source
    end                                                     as media_type
  , artist_music_url                                        as media_url
  , row_number() over ( partition by artist_id order by case when artist_music_source = 'spotify' then 1 when artist_music_source = 'bandcamp' then 2 else 3 end ) as url_rn
from 
  {{ ref('dim_artist_music') }}
),
artist_music as 
( select
    artist_id
  , array_agg ( struct ( media_url, media_type ) ) as artist_music
from 
  music_ranked
where
  url_rn <= 3
group by 
  artist_id 
) 
select 
    artist.artist_id 
  , artist.artist_name 
  , artist.artist_website_url 
  , artist_music.artist_music
  , struct ( 
        facebook.artist_social_url  as facebook
      , instagram.artist_social_url as instagram
  )                                                 as artist_social
from 
  {{ ref('dim_artist') }} artist 
  left join
  artist_music 
    using ( artist_id ) 
  left join
  {{ ref('dim_artist_social') }} facebook
    on artist.artist_id = facebook.artist_id
    and facebook.social_media_website = 'facebook'
  left join
  {{ ref('dim_artist_social') }} instagram
    on artist.artist_id = facebook.artist_id
    and facebook.social_media_website = 'instagram'
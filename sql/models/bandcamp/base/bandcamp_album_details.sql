with dup_data as
( select
    to_hex ( {{ dbt_utils.surrogate_key('bandcamp_album_url') }} )                as bandcamp_album_id
  , bandcamp_album_url
  , bandcamp_embedded_player_link
  , bandcamp_artist_location
  , trim ( replace ( bio, '... more', '' ) )                                      as bandcamp_artist_bio
  , band_links
  , band_showography
  , extract_ts
  , row_number() over ( partition by bandcamp_album_url order by _file_name desc ) as dup_rn
from
  {{ source('bandcamp', 'bandcamp_album_details') }}
)
select
    bandcamp_album_id
  , bandcamp_album_url
  , bandcamp_embedded_player_link
  , bandcamp_artist_location
  , bandcamp_artist_bio
  , band_links
  , band_showography
  , extract_ts
from
  dup_data
where
  dup_rn = 1

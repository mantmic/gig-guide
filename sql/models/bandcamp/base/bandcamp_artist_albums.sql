with dup_data as
( select
    bandcamp_url
  , bandcamp_album_url                                                             as bandcamp_album_url
  , to_hex ( {{ dbt_utils.surrogate_key('bandcamp_url') }} )                       as bandcamp_artist_id
  , row_number() over ( partition by bandcamp_album_url order by _file_name desc ) as dup_rn
from
  {{ source('bandcamp', 'bandcamp_artist_albums') }}
)
select
    bandcamp_url
  , bandcamp_album_url
  , to_hex ( {{ dbt_utils.surrogate_key('bandcamp_album_url') }} ) as bandcamp_album_id
  , bandcamp_artist_id
from
  dup_data
where
  dup_rn = 1

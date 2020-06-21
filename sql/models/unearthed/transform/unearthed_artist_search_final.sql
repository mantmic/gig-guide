{{ config(materialized='table') }}

with search as
( select
    details.unearthed_artist_id
  , details.unearthed_artist_url
  , details.unearthed_artist_name
  , details.artist_location
  , lower ( trim ( search.unearthed_search_artist_name ) )  as search_artist_name
  , details.artist_website                                  as artist_website_url
  , case when trim ( lower ( search.unearthed_search_artist_name ) ) = trim ( lower ( details.unearthed_artist_name  ) ) then 'Y'
         else 'N'
    end as name_match_yn
  , search.unearthed_search_artist_sk
  , search.unearthed_search_result_order
  , search.unearthed_search_result_type
from
  {{ ref('unearthed_artist_details') }} details
  join
  {{ ref('unearthed_artist_search') }} search
    using ( unearthed_artist_url )
)
select
    unearthed_artist_id
  , unearthed_artist_url
  , unearthed_artist_name
  , artist_location
  , search_artist_name
  , artist_website_url
  , name_match_yn
  , unearthed_search_artist_sk
  , unearthed_search_result_order
  , unearthed_search_result_type
  , row_number() over ( partition by search_artist_name order by name_match_yn desc, unearthed_search_result_order ) as search_rank_order
from
  search

{{ config(materialized='view') }}

select 
    region.region_name 
  , venue.venue_name 
  , gig.gig_date
  , format_date (  '%A', gig.gig_date ) as day_of_week
  , gig.gig_url 
from 
  {{ ref('dim_venue') }} venue 
  join
  {{ ref('mapping_social_post_region') }} region 
    on ST_CONTAINS ( ST_GEOGFROMTEXT ( region.region_wkt ), venue.location_geog )
  join
  {{ ref('dim_gig') }} gig
    using ( venue_id ) 
where
  gig.gig_date between current_date ( 'Australia/Sydney' ) and date_add ( current_date ( 'Australia/Sydney' ), interval 7 day )
  and gig.gig_cancelled_yn != 'Y'
order by
  gig.gig_date
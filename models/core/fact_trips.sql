{{
    config(
        materialized='table'
    )
}}
with fhv_tripdata as (
    select *, 
        'FHV' as service_type
    from {{ ref('stg_staging__fhv_data') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select tripid,
dispatching_base_num,
pickup_locationid,
dropoff_locationid,
dropoff_zone.borough as dropoff_borough, 
dropoff_zone.zone as dropoff_zone,  
pickup_datetime,
dropoff_datetime,
sr_flag,
affiliated_base_number,
service_type
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
where 1=1
and dropoff_borough is not null
and dropoff_zone is not null
{{
    config(
        materialized='table'
    )
}}

with green_trips as (
    select 
        pickup_locationid,
        date_trunc(date(pickup_datetime), month) as revenue_month,
        service_type,
        sum(fare_amount) as revenue_monthly_fare,
        sum(extra) as revenue_monthly_extra,
        sum(mta_tax) as revenue_monthly_mta_tax,
        sum(tip_amount) as revenue_monthly_tip_amount,
        sum(tolls_amount) as revenue_monthly_tolls_amount,
        sum(ehail_fee) as revenue_monthly_ehail_fee,
        sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
        sum(total_amount) as revenue_monthly_total_amount,
        sum(congestion_surcharge) as revenue_monthly_congestion_surcharge,
        count(tripid) as total_monthly_trips,
        avg(passenger_count) as avg_monthly_passenger_count,
        avg(trip_distance) as avg_monthly_trip_distance
    from {{ ref('stg_green_tripdata') }}
    where pickup_datetime is not null
    group by 1, 2, 3
),

yellow_trips as (
    select 
        pickup_locationid,
        date_trunc(date(pickup_datetime), month) as revenue_month,
        service_type,
        sum(fare_amount) as revenue_monthly_fare,
        sum(extra) as revenue_monthly_extra,
        sum(mta_tax) as revenue_monthly_mta_tax,
        sum(tip_amount) as revenue_monthly_tip_amount,
        sum(tolls_amount) as revenue_monthly_tolls_amount,
        sum(ehail_fee) as revenue_monthly_ehail_fee,
        sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
        sum(total_amount) as revenue_monthly_total_amount,
        sum(congestion_surcharge) as revenue_monthly_congestion_surcharge,
        count(tripid) as total_monthly_trips,
        avg(passenger_count) as avg_monthly_passenger_count,
        avg(trip_distance) as avg_monthly_trip_distance
    from {{ ref('stg_yellow_tripdata') }}
    where pickup_datetime is not null
    group by 1, 2, 3
),

fhv_trips as (
    select 
        pickup_locationid,
        date_trunc(date(pickup_datetime), month) as revenue_month,
        service_type,
        0 as revenue_monthly_fare,
        0 as revenue_monthly_extra,
        0 as revenue_monthly_mta_tax,
        0 as revenue_monthly_tip_amount,
        0 as revenue_monthly_tolls_amount,
        0 as revenue_monthly_ehail_fee,
        0 as revenue_monthly_improvement_surcharge,
        0 as revenue_monthly_total_amount,
        0 as revenue_monthly_congestion_surcharge,
        count(tripid) as total_monthly_trips,
        null as avg_monthly_passenger_count,
        null as avg_monthly_trip_distance
    from {{ ref('stg_fhv_tripdata') }}
    where pickup_datetime is not null
    group by 1, 2, 3
),

all_trips as (
    select * from green_trips
    union all
    select * from yellow_trips
    union all
    select * from fhv_trips
)

select 
    pickup_zone.zone as revenue_zone,
    all_trips.revenue_month,
    all_trips.service_type,
    all_trips.revenue_monthly_fare,
    all_trips.revenue_monthly_extra,
    all_trips.revenue_monthly_mta_tax,
    all_trips.revenue_monthly_tip_amount,
    all_trips.revenue_monthly_tolls_amount,
    all_trips.revenue_monthly_ehail_fee,
    all_trips.revenue_monthly_improvement_surcharge,
    all_trips.revenue_monthly_total_amount,
    all_trips.revenue_monthly_congestion_surcharge,
    all_trips.total_monthly_trips,
    all_trips.avg_monthly_passenger_count,
    all_trips.avg_monthly_trip_distance
from all_trips
inner join {{ ref('dim_zones') }} as pickup_zone
on all_trips.pickup_locationid = pickup_zone.locationid

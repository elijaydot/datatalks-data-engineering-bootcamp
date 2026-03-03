{{
    config(
        materialized='view'
    )
}}

with green_data as (
    select 
        tripid,
        cast(vendorid as string) as vendorid,
        ratecodeid,
        pickup_locationid,
        dropoff_locationid,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        congestion_surcharge,
        service_type
    from {{ ref('stg_green_tripdata') }}
), 

yellow_data as (
    select 
        tripid,
        cast(vendorid as string) as vendorid,
        ratecodeid,
        pickup_locationid,
        dropoff_locationid,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        null as trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        congestion_surcharge,
        service_type
    from {{ ref('stg_yellow_tripdata') }}
), 

fhv_data as (
    select 
        tripid,
        cast(dispatching_base_num as string) as vendorid,
        cast(null as int64) as ratecodeid,
        pickup_locationid,
        dropoff_locationid,
        pickup_datetime,
        dropoff_datetime,
        cast(null as string) as store_and_fwd_flag,
        cast(null as int64) as passenger_count,
        cast(null as numeric) as trip_distance,
        cast(null as int64) as trip_type,
        cast(null as numeric) as fare_amount,
        cast(null as numeric) as extra,
        cast(null as numeric) as mta_tax,
        cast(null as numeric) as tip_amount,
        cast(null as numeric) as tolls_amount,
        cast(null as numeric) as ehail_fee,
        cast(null as numeric) as improvement_surcharge,
        cast(null as numeric) as total_amount,
        cast(null as int64) as payment_type,
        cast(null as numeric) as congestion_surcharge,
        service_type
    from {{ ref('stg_fhv_tripdata') }}
), 

trips_unioned as (
    select * from green_data
    union all
    select * from yellow_data
    union all
    select * from fhv_data
) 

select * from trips_unioned

/*
TODO 
- ONE ROW PER TRIP (doesnt matter if yellow or green)
- add a primary key {trip_id}. It has to be unique. 
- Find all the duplicates, understand why they are duplicated, and then decide how to handle them.
- Find a way to enrich the colume payment_type
*/

{{ config(materialized='table') }}

with trips as (
    select * from {{ ref('int_trips_unioned') }}
),

-- 중복 제거: 동일 vendor, 시간, 위치, 거리 조합의 첫 번째 행만 유지
deduped as (
    select
        *,
        row_number() over (
            partition by vendor_id, pickup_datetime, dropoff_datetime,
                         pickup_location_id, dropoff_location_id, trip_distance
            order by pickup_datetime
        ) as rn
    from trips
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
)

select
    -- primary key: 각 행에 고유한 trip_id 생성
    {{ dbt_utils.generate_surrogate_key([
        'vendor_id',
        'pickup_datetime',
        'dropoff_datetime',
        'pickup_location_id',
        'dropoff_location_id',
        'trip_distance'
    ]) }} as trip_id,

    -- identifiers
    d.vendor_id,
    d.ratecode_id,
    d.pickup_location_id,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    d.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,

    -- timestamps
    d.pickup_datetime,
    d.dropoff_datetime,

    -- trip info
    d.store_and_fwd_flag,
    d.passenger_count,
    d.trip_distance,
    d.trip_type,

    -- payment info
    d.fare_amount,
    d.extra,
    d.mta_tax,
    d.tip_amount,
    d.tolls_amount,
    d.ehail_fee,
    d.improvement_surcharge,
    d.total_amount,
    d.payment_type,
    case
        when d.payment_type = 1 then 'Credit card'
        when d.payment_type = 2 then 'Cash'
        when d.payment_type = 3 then 'No charge'
        when d.payment_type = 4 then 'Dispute'
        when d.payment_type = 5 then 'Unknown'
        when d.payment_type = 6 then 'Voided trip'
        else 'Other'
    end as payment_type_description,

    -- service type
    d.service_type

from deduped d
left join dim_zones pickup_zone
    on d.pickup_location_id = pickup_zone.location_id
left join dim_zones dropoff_zone
    on d.dropoff_location_id = dropoff_zone.location_id
where d.rn = 1
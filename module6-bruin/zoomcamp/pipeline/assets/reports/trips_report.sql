/* @bruin

name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: trip_date
  time_granularity: date

columns:
  - name: trip_date
    type: date
    description: "Date of trips"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow/green)"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Payment method name"
    primary_key: true
  - name: trip_count
    type: bigint
    description: "Number of trips"
    checks:
      - name: non_negative
  - name: total_passengers
    type: bigint
    description: "Total number of passengers"
    checks:
      - name: non_negative
  - name: total_distance
    type: float
    description: "Total trip distance in miles"
    checks:
      - name: non_negative
  - name: total_fare
    type: float
    description: "Total fare amount"
    checks:
      - name: non_negative
  - name: total_tips
    type: float
    description: "Total tip amount"
    checks:
      - name: non_negative
  - name: total_revenue
    type: float
    description: "Total revenue (fares + tips + fees)"
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: float
    description: "Average trip distance"
  - name: avg_fare_per_trip
    type: float
    description: "Average fare per trip"

@bruin */

-- Reports: Daily aggregation by taxi type and payment method
SELECT
    CAST(pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    COALESCE(payment_type_name, 'unknown') AS payment_type_name,
    COUNT(*) AS trip_count,
    COALESCE(SUM(CAST(passenger_count AS BIGINT)), 0) AS total_passengers,
    COALESCE(SUM(trip_distance), 0) AS total_distance,
    COALESCE(SUM(fare_amount), 0) AS total_fare,
    COALESCE(SUM(tip_amount), 0) AS total_tips,
    COALESCE(SUM(total_amount), 0) AS total_revenue,
    COALESCE(AVG(trip_distance), 0) AS avg_trip_distance,
    COALESCE(AVG(fare_amount), 0) AS avg_fare_per_trip
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
    CAST(pickup_datetime AS DATE),
    taxi_type,
    COALESCE(payment_type_name, 'unknown')

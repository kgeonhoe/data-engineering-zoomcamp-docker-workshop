/* @bruin

name: staging.trips

type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup timestamp"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff timestamp"
    primary_key: true
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "TLC Taxi Zone pickup location"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: "TLC Taxi Zone dropoff location"
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "Base fare amount"
    primary_key: true
    checks:
      - name: not_null
  - name: total_amount
    type: float
    description: "Total trip amount"
    checks:
      - name: non_negative
  - name: payment_type_name
    type: string
    description: "Payment type name from lookup"

custom_checks:
  - name: no_duplicate_trips
    description: "Ensure no duplicate trips after deduplication"
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT (pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount))
      FROM staging.trips
      WHERE pickup_datetime >= '{{ start_datetime }}'
        AND pickup_datetime < '{{ end_datetime }}'
    value: 0

@bruin */

-- Staging: Clean, deduplicate, and enrich trip data
WITH deduplicated AS (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                t.pickup_datetime,
                t.dropoff_datetime,
                t.pickup_location_id,
                t.dropoff_location_id,
                t.fare_amount
            ORDER BY t.extracted_at DESC
        ) AS row_num
    FROM ingestion.trips t
    WHERE t.pickup_datetime >= '{{ start_datetime }}'
      AND t.pickup_datetime < '{{ end_datetime }}'
      AND t.pickup_datetime IS NOT NULL
      AND t.dropoff_datetime IS NOT NULL
      AND t.pickup_location_id IS NOT NULL
      AND t.dropoff_location_id IS NOT NULL
      AND t.fare_amount IS NOT NULL
      AND t.fare_amount >= 0
      AND t.total_amount >= 0
)

SELECT
    d.vendor_id,
    d.pickup_datetime,
    d.dropoff_datetime,
    d.passenger_count,
    d.trip_distance,
    d.pickup_location_id,
    d.dropoff_location_id,
    d.rate_code_id,
    d.store_and_fwd_flag,
    d.payment_type,
    p.payment_type_name,
    d.fare_amount,
    d.extra,
    d.mta_tax,
    d.tip_amount,
    d.tolls_amount,
    d.improvement_surcharge,
    d.total_amount,
    d.congestion_surcharge,
    d.airport_fee,
    d.taxi_type,
    d.extracted_at
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup p
    ON d.payment_type = p.payment_type_id
WHERE d.row_num = 1

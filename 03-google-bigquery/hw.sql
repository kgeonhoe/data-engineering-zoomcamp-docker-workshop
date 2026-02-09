-- ============================================
-- Question 1: Record count for 2024 Yellow Taxi
-- ============================================

-- Step 1: External Table 생성
CREATE OR REPLACE EXTERNAL TABLE `nimble-courier-485614-g9.ny_taxi_dataset.external_yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nimble-courier-485614-g9-data-lake/yellow/yellow_tripdata_2024-*.parquet']
);

-- Step 2: Materialized Table 생성
CREATE OR REPLACE TABLE `nimble-courier-485614-g9.ny_taxi_dataset.yellow_tripdata_2024`
AS SELECT * FROM `nimble-courier-485614-g9.ny_taxi_dataset.external_yellow_tripdata_2024`;

-- Step 3: count 쿼리 → 답: 20,332,093
SELECT count(*) FROM `nimble-courier-485614-g9.ny_taxi_dataset.yellow_tripdata_2024`;


-- ============================================
-- Question 2: Estimated data for PULocationID query
-- External Table vs Materialized Table
-- ============================================

-- External Table에서 실행 (우측 상단의 estimated bytes 확인)
SELECT DISTINCT PULocationID
FROM `nimble-courier-485614-g9.ny_taxi_dataset.external_yellow_tripdata_2024`;

-- Materialized Table에서 실행 (우측 상단의 estimated bytes 확인)
SELECT DISTINCT PULocationID
FROM `nimble-courier-485614-g9.ny_taxi_dataset.yellow_tripdata_2024`;
-- 답: 0 MB for the External Table and 155.12 MB for the Materialized Table
-- (External Table은 Parquet이라 BigQuery가 추정하지 못해 0으로 표시)


-- ============================================
-- Question 3: Why are estimated bytes different?
-- ============================================
-- 답: BigQuery is a columnar database, and it only scans the specific columns
--     requested in the query. Querying two columns (PULocationID, DOLocationID)
--     requires reading more data than querying one column (PULocationID),
--     leading to a higher estimated number of bytes processed.


-- ============================================
-- Question 4: Records with fare_amount = 0
-- ============================================
SELECT count(*) FROM `nimble-courier-485614-g9.ny_taxi_dataset.yellow_tripdata_2024`
WHERE fare_amount = 0;
-- 답: 8,333


-- ============================================
-- Question 5: Best optimization strategy
-- (filter on tpep_dropoff_datetime, order by VendorID)
-- ============================================
-- 답: Partition by tpep_dropoff_datetime and Cluster on VendorID

CREATE OR REPLACE TABLE `nimble-courier-485614-g9.ny_taxi_dataset.yellow_tripdata_2024_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `nimble-courier-485614-g9.ny_taxi_dataset.yellow_tripdata_2024`;


-- ============================================
-- Question 6: Estimated bytes for distinct VendorID query
-- between tpep_dropoff_datetime 2024-03-01 and 2024-03-15
-- ============================================

-- Non-partitioned (materialized) table
SELECT DISTINCT VendorID
FROM `nimble-courier-485614-g9.ny_taxi_dataset.yellow_tripdata_2024`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Partitioned + clustered table
SELECT DISTINCT VendorID
FROM `nimble-courier-485614-g9.ny_taxi_dataset.yellow_tripdata_2024_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
-- 답: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table


-- ============================================
-- Question 7: Where is data stored in External Table?
-- ============================================
-- 답: GCP Bucket


-- ============================================
-- Question 8: Is it best practice to always cluster?
-- ============================================
-- 답: False (소규모 테이블에서는 오히려 오버헤드)


-- ============================================
-- Question 9 (bonus): SELECT count(*) from materialized table
-- ============================================
SELECT count(*) FROM `nimble-courier-485614-g9.ny_taxi_dataset.yellow_tripdata_2024`;
-- 답: 0 bytes (BigQuery는 메타데이터에 행 수를 저장하므로 데이터 스캔 불필요)
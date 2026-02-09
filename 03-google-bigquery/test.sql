-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `nimble-courier-485614-g9.ny_taxi_dataset.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nimble-courier-485614-g9-data-lake/green/green_tripdata_2019-*.parquet', 
          'gs://nimble-courier-485614-g9-data-lake/green/green_tripdata_2020-*.parquet']
);

-- Check green trip data
SELECT * FROM `nimble-courier-485614-g9.ny_taxi_dataset.external_green_tripdata` LIMIT 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `nimble-courier-485614-g9.ny_taxi_dataset.green_tripdata_non_partitioned` AS
SELECT * FROM `nimble-courier-485614-g9.ny_taxi_dataset.external_green_tripdata`;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `nimble-courier-485614-g9.ny_taxi_dataset.green_tripdata_partitioned`
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM `nimble-courier-485614-g9.ny_taxi_dataset.external_green_tripdata`;

-- Impact of partition
-- Scanning full data
SELECT DISTINCT(VendorID)
FROM `nimble-courier-485614-g9.ny_taxi_dataset.green_tripdata_non_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning partitioned data (much less)
SELECT DISTINCT(VendorID)
FROM `nimble-courier-485614-g9.ny_taxi_dataset.green_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Partitioned + clustered table
CREATE OR REPLACE TABLE `nimble-courier-485614-g9.ny_taxi_dataset.green_tripdata_partitioned_clustered`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `nimble-courier-485614-g9.ny_taxi_dataset.external_green_tripdata`;



CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.nytaxi.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nimble-courier-485614-g9-data-lake/trip data/fhv_tripdata_2019-*.csv']
);

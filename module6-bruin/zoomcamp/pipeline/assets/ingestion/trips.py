"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: vendor_id
    type: integer
    description: "Vendor identifier"
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup timestamp"
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff timestamp"
  - name: passenger_count
    type: float
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
  - name: pickup_location_id
    type: integer
    description: "TLC Taxi Zone pickup location"
  - name: dropoff_location_id
    type: integer
    description: "TLC Taxi Zone dropoff location"
  - name: rate_code_id
    type: float
    description: "Rate code in effect"
  - name: store_and_fwd_flag
    type: string
    description: "Store and forward flag"
  - name: payment_type
    type: integer
    description: "Payment type code"
  - name: fare_amount
    type: float
    description: "Base fare amount"
  - name: extra
    type: float
    description: "Extra charges"
  - name: mta_tax
    type: float
    description: "MTA tax"
  - name: tip_amount
    type: float
    description: "Tip amount"
  - name: tolls_amount
    type: float
    description: "Tolls amount"
  - name: improvement_surcharge
    type: float
    description: "Improvement surcharge"
  - name: total_amount
    type: float
    description: "Total trip amount"
  - name: congestion_surcharge
    type: float
    description: "Congestion surcharge"
  - name: airport_fee
    type: float
    description: "Airport fee"
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow/green)"
  - name: extracted_at
    type: timestamp
    description: "Timestamp when data was extracted"

@bruin"""

import os
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


def materialize():
    """
    Fetch NYC Taxi trip data from TLC public endpoint.
    Uses BRUIN_START_DATE/BRUIN_END_DATE for date range and
    BRUIN_VARS for taxi_types configuration.
    """
    # Get date range from Bruin environment variables
    start_date = os.environ.get("BRUIN_START_DATE", "2022-01-01")
    end_date = os.environ.get("BRUIN_END_DATE", "2022-02-01")

    # Parse taxi_types from BRUIN_VARS
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", '{"taxi_types": ["yellow"]}'))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    # Base URL for NYC TLC trip data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    # Generate list of months to fetch
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    all_data = []
    current = start

    while current < end:
        year_month = current.strftime("%Y-%m")

        for taxi_type in taxi_types:
            url = f"{base_url}/{taxi_type}_tripdata_{year_month}.parquet"
            print(f"Fetching: {url}")

            try:
                df = pd.read_parquet(url)

                # Standardize column names based on taxi type
                column_mapping = {
                    # Yellow taxi columns
                    "VendorID": "vendor_id",
                    "tpep_pickup_datetime": "pickup_datetime",
                    "tpep_dropoff_datetime": "dropoff_datetime",
                    # Green taxi columns
                    "lpep_pickup_datetime": "pickup_datetime",
                    "lpep_dropoff_datetime": "dropoff_datetime",
                    # Common columns
                    "passenger_count": "passenger_count",
                    "trip_distance": "trip_distance",
                    "PULocationID": "pickup_location_id",
                    "DOLocationID": "dropoff_location_id",
                    "RatecodeID": "rate_code_id",
                    "store_and_fwd_flag": "store_and_fwd_flag",
                    "payment_type": "payment_type",
                    "fare_amount": "fare_amount",
                    "extra": "extra",
                    "mta_tax": "mta_tax",
                    "tip_amount": "tip_amount",
                    "tolls_amount": "tolls_amount",
                    "improvement_surcharge": "improvement_surcharge",
                    "total_amount": "total_amount",
                    "congestion_surcharge": "congestion_surcharge",
                    "Airport_fee": "airport_fee",
                    "airport_fee": "airport_fee",
                }

                df = df.rename(columns=column_mapping)

                # Select only the columns we need
                expected_cols = [
                    "vendor_id", "pickup_datetime", "dropoff_datetime",
                    "passenger_count", "trip_distance", "pickup_location_id",
                    "dropoff_location_id", "rate_code_id", "store_and_fwd_flag",
                    "payment_type", "fare_amount", "extra", "mta_tax",
                    "tip_amount", "tolls_amount", "improvement_surcharge",
                    "total_amount", "congestion_surcharge", "airport_fee"
                ]

                # Keep only columns that exist
                available_cols = [c for c in expected_cols if c in df.columns]
                df = df[available_cols]

                # Add metadata columns
                df["taxi_type"] = taxi_type
                df["extracted_at"] = datetime.utcnow()

                all_data.append(df)
                print(f"  Loaded {len(df):,} rows for {taxi_type} {year_month}")

            except Exception as e:
                print(f"  Warning: Could not fetch {url}: {e}")

        # Move to next month
        current = current + relativedelta(months=1)

    if not all_data:
        print("No data fetched")
        return pd.DataFrame()

    # Concatenate all dataframes
    final_df = pd.concat(all_data, ignore_index=True)
    print(f"Total rows fetched: {len(final_df):,}")

    return final_df

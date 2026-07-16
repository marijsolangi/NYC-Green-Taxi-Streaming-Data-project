#!/usr/bin/env python3
"""
================================================================================
NYC GREEN TAXI STREAMING - PHASE 2: DATA EXPLORATION & KAFKA CONNECTION
================================================================================
Author: Muhammad Marij
Email: mohammedmarij@gmail.com

This script covers:
  - Question 1: Testing Kafka connection via KafkaProducer
  - Question 2: Loading and exploring the Green Taxi dataset

Prerequisites:
  pip install kafka-python pandas
"""

import json
import pandas as pd
from kafka import KafkaProducer

# =============================================================================
# QUESTION 1: CONNECTING TO THE KAFKA SERVER
# =============================================================================

def json_serializer(data):
    """
    Serialize Python dictionary to JSON-encoded bytes.
    This serializer is used by KafkaProducer to convert Python objects
    into the byte format required by Kafka.
    """
    return json.dumps(data).encode('utf-8')


# Kafka server configuration
# 'localhost:9092' is the default Redpanda/Kafka listener exposed by Docker
server = 'localhost:9092'

# Create a KafkaProducer instance
# - bootstrap_servers: list of Kafka broker addresses to connect to
# - value_serializer: function to serialize message values before sending
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

# Test the connection by sending a test message to a dummy topic
# If this succeeds without errors, the connection is working
test_message = {
    "test": "connection",
    "status": "ok",
    "timestamp": pd.Timestamp.now().isoformat()
}

# Send test message (asynchronous - returns a Future)
future = producer.send('test-topic', test_message)

# Wait for the send to complete and get metadata
record_metadata = future.get(timeout=10)

print("=" * 60)
print("QUESTION 1: KAFKA CONNECTION TEST")
print("=" * 60)
print(f"Kafka server: {server}")
print(f"Connected successfully!")
print(f"Test message sent to topic: {record_metadata.topic}")
print(f"Partition: {record_metadata.partition}")
print(f"Offset: {record_metadata.offset}")
print("=" * 60)

# Flush any pending messages and close the producer
producer.flush()
producer.close()


# =============================================================================
# QUESTION 2: EXPLORING THE DATASET
# =============================================================================

print("\n" + "=" * 60)
print("QUESTION 2: DATASET EXPLORATION")
print("=" * 60)

# Load the Green Taxi dataset from the compressed CSV file
# The dataset contains NYC Green Taxi trip records for October 2019
# Source: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
df = pd.read_csv('green_tripdata_2019-10.csv.gz', compression='gzip')

print(f"Total rows in dataset: {len(df):,}")
print(f"Total columns: {len(df.columns)}")
print(f"\nColumn names:")
print(df.columns.tolist())

# Select only the necessary columns for our streaming pipeline
# These columns represent the core trip information we want to analyze
cols = [
    'lpep_pickup_datetime',    # When the passenger was picked up (event time)
    'lpep_dropoff_datetime',   # When the passenger was dropped off (used for session windowing)
    'PULocationID',             # Pickup location zone ID (NYC taxi zones)
    'DOLocationID',             # Drop-off location zone ID
    'passenger_count',          # Number of passengers
    'trip_distance',            # Trip distance in miles
    'tip_amount'                # Tip amount in dollars
]

# Filter the DataFrame to only include selected columns
df_green = df[cols].copy()

print(f"\nFiltered DataFrame shape: {df_green.shape}")
print(f"\nFirst 5 rows:")
print(df_green.head())

print(f"\nData types:")
print(df_green.dtypes)

print(f"\nBasic statistics:")
print(df_green.describe())

# =============================================================================
# CONVERT ROWS TO DICTIONARY FORMAT (for Kafka serialization)
# =============================================================================

print("\n" + "=" * 60)
print("SAMPLE ROW AS DICTIONARY (Kafka message format)")
print("=" * 60)

# Iterate through rows using itertuples (faster than iterrows)
# Each row is converted to a dictionary with column names as keys
for row in df_green.itertuples(index=False):
    # Build dictionary: {column_name: value}
    row_dict = {col: getattr(row, col) for col in row._fields}
    print("Sample row dictionary:")
    print(json.dumps(row_dict, indent=2, default=str))
    break  # Only print the first row as sample

print("=" * 60)
print("Phase 2 complete. Data ready for Kafka streaming.")
print("=" * 60)

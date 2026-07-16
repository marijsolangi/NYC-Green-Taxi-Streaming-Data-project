#!/usr/bin/env python3
"""
================================================================================
NYC GREEN TAXI STREAMING - PHASE 3: PRODUCING DATA TO KAFKA
================================================================================
File: load_taxi_data.py
Author: Muhammad Marij
Email: mohammedmarij@gmail.com

This script:
  1. Creates a Kafka topic named 'green-trips'
  2. Loads the filtered Green Taxi dataset
  3. Sends each trip record as a JSON message to the topic
  4. Measures and reports the total transmission time (excluding delays)

Usage:
  python load_taxi_data.py

Prerequisites:
  - pip install kafka-python pandas
  - Redpanda/Kafka running on localhost:9092
  - green_tripdata_2019-10.csv.gz in the same directory
"""

import json
import time
import pandas as pd
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

# =============================================================================
# CONFIGURATION
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'green-trips'
CSV_FILE = 'green_tripdata_2019-10.csv.gz'

# Columns to extract from the dataset
COLUMNS = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]


# =============================================================================
# KAFKA SETUP: CREATE TOPIC
# =============================================================================

def create_topic(topic_name: str, bootstrap_servers: list) -> None:
    """
    Create a Kafka topic programmatically.

    Args:
        topic_name: Name of the topic to create
        bootstrap_servers: List of Kafka broker addresses
    """
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    topic = NewTopic(
        name=topic_name,
        num_partitions=1,        # Single partition for ordered processing
        replication_factor=1       # Single replica (local development)
    )

    try:
        admin_client.create_topics([topic])
        print(f"Topic '{topic_name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists. Using existing topic.")
    finally:
        admin_client.close()


def json_serializer(data: dict) -> bytes:
    """
    Serialize a Python dictionary to JSON-encoded bytes.

    This function is passed to KafkaProducer as the value_serializer.
    It converts each trip record dictionary into a JSON string,
    then encodes it as UTF-8 bytes for Kafka transmission.
    """
    return json.dumps(data, default=str).encode('utf-8')


# =============================================================================
# DATA LOADING
# =============================================================================

def load_taxi_data(csv_file: str, columns: list) -> pd.DataFrame:
    """
    Load and filter the Green Taxi dataset.

    Args:
        csv_file: Path to the compressed CSV file
        columns: List of columns to retain

    Returns:
        Filtered DataFrame
    """
    print(f"Loading data from {csv_file}...")
    df = pd.read_csv(csv_file, compression='gzip')
    df_filtered = df[columns].copy()
    print(f"Loaded {len(df_filtered):,} records with {len(columns)} columns.")
    return df_filtered


# =============================================================================
# MAIN: SEND DATA TO KAFKA WITH TIMING
# =============================================================================

def main():
    print("=" * 60)
    print("NYC GREEN TAXI - KAFKA DATA PRODUCER")
    print("=" * 60)

    # Step 1: Create the Kafka topic
    print("\n[1/4] Creating Kafka topic...")
    create_topic(TOPIC_NAME, KAFKA_BOOTSTRAP_SERVERS)

    # Step 2: Load the dataset
    print("\n[2/4] Loading taxi dataset...")
    df_green = load_taxi_data(CSV_FILE, COLUMNS)

    # Step 3: Initialize Kafka Producer
    print("\n[3/4] Initializing Kafka producer...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=json_serializer,
        # Batch settings for performance
        batch_size=16384,           # 16KB batches
        linger_ms=5,                # Wait up to 5ms to batch
        compression_type='gzip'     # Compress messages
    )
    print("Producer connected to Kafka.")

    # Step 4: Send all records with precise timing
    print("\n[4/4] Sending records to Kafka...")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Total records: {len(df_green):,}")
    print("-" * 60)

    # START TIMING: Record the exact moment before sending begins
    # We use time.perf_counter() for high-precision timing
    start_time = time.perf_counter()

    records_sent = 0
    total_records = len(df_green)

    # Iterate through each row and send to Kafka
    # itertuples() is faster than iterrows() for large datasets
    for row in df_green.itertuples(index=False):
        # Convert row to dictionary: {column_name: value}
        row_dict = {col: getattr(row, col) for col in row._fields}

        # Send message asynchronously (non-blocking)
        # The producer batches messages internally for efficiency
        producer.send(TOPIC_NAME, value=row_dict)

        records_sent += 1

        # Print progress every 10,000 records
        if records_sent % 10000 == 0:
            elapsed = time.perf_counter() - start_time
            print(f"  Progress: {records_sent:,}/{total_records:,} "
                  f"({records_sent/total_records*100:.1f}%) - "
                  f"{elapsed:.1f}s elapsed")

    # Ensure all messages are sent before stopping timing
    # flush() blocks until all pending messages are delivered
    producer.flush()

    # STOP TIMING: Record the exact moment after all messages are sent
    end_time = time.perf_counter()

    # Calculate total duration (rounded to whole number of seconds)
    total_duration = round(end_time - start_time)

    # Close the producer connection
    producer.close()

    # Print final results
    print("-" * 60)
    print("TRANSMISSION COMPLETE")
    print("=" * 60)
    print(f"Total records sent: {records_sent:,}")
    print(f"Total duration: {total_duration} seconds")
    print(f"Throughput: {records_sent/total_duration:,.0f} records/second")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Kafka server: {KAFKA_BOOTSTRAP_SERVERS[0]}")
    print("=" * 60)


if __name__ == "__main__":
    main()

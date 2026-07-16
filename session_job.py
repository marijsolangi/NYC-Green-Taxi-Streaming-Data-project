#!/usr/bin/env python3
"""
================================================================================
NYC GREEN TAXI STREAMING - PHASE 4: FLINK SESSIONIZATION WINDOW
================================================================================
File: session_job.py
Author: Muhammad Marij
Email: mohammedmarij@gmail.com

This script implements a Flink streaming job that:
  1. Reads JSON taxi trip data from the 'green-trips' Kafka topic
  2. Parses and assigns event time using lpep_dropoff_datetime
  3. Applies a 5-second watermark for handling out-of-order events
  4. Groups trips by (PULocationID, DOLocationID) pairs
  5. Uses SESSION windows with a 5-minute gap to find "unbroken streaks"
  6. Identifies the pickup/drop-off pairs with the longest streaks

SESSION WINDOW CONCEPT:
-----------------------
A session window groups events by periods of activity separated by gaps
of inactivity. Unlike tumbling windows (fixed size), session windows:
  - Have dynamic start and end times
  - Extend as long as events arrive within the gap duration
  - Close when no event arrives for longer than the gap

In our taxi context:
  - A "streak" = a session window
  - If trips between the same locations keep happening within 5 minutes,
    the session continues (streak grows)
  - If no trip occurs for >5 minutes, the session closes (streak ends)
  - The longest session = longest unbroken streak

Prerequisites:
  - pip install apache-flink
  - Kafka connector JAR: flink-sql-connector-kafka-3.1.0-1.18.jar
  - Redpanda/Kafka running on localhost:9092
  - 'green-trips' topic populated with data

Usage:
  python session_job.py
"""

import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    EnvironmentSettings,
    DataTypes,
    Schema
)
from pyflink.table.expressions import col, lit
from pyflink.table.window import Session

# =============================================================================
# CONFIGURATION
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "green-trips"
KAFKA_GROUP_ID = "flink-session-window-job"

# Path to the Flink Kafka connector JAR
# Download from: https://repo.maven.apache.org/maven2/org/apache/flink/
# Example: flink-sql-connector-kafka-3.1.0-1.18.jar
FLINK_KAFKA_JAR = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    "flink-sql-connector-kafka-3.1.0-1.18.jar"
)

# Watermark and session configuration
WATERMARK_SECONDS = 5      # Allow 5 seconds of out-of-orderness
SESSION_GAP_MINUTES = 5    # Session closes after 5 minutes of inactivity


# =============================================================================
# FLINK ENVIRONMENT SETUP
# =============================================================================

def setup_flink_environment():
    """
    Initialize the Flink streaming environment with Kafka connector.

    Returns:
        StreamTableEnvironment: Configured Flink table environment
    """
    # Create the streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Single parallelism for local development

    # Add the Kafka connector JAR to the classpath
    # This JAR provides the Kafka source/sink connectors for Flink
    if os.path.exists(FLINK_KAFKA_JAR):
        env.add_jars(f"file://{FLINK_KAFKA_JAR}")
        print(f"Added Kafka connector JAR: {FLINK_KAFKA_JAR}")
    else:
        print(f"WARNING: Kafka connector JAR not found at {FLINK_KAFKA_JAR}")
        print("Please download it from Maven Central and place it in the script directory.")

    # Create table environment in streaming mode
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, settings)

    return t_env


# =============================================================================
# KAFKA SOURCE TABLE DEFINITION
# =============================================================================

def create_kafka_source_table(t_env):
    """
    Create a temporary table that reads from the 'green-trips' Kafka topic.

    The schema maps JSON fields from Kafka messages to Flink table columns.
    Event time is extracted from lpep_dropoff_datetime with a watermark.

    Args:
        t_env: Flink table environment
    """
    # DDL to create the Kafka source table
    # Key configurations:
    #   - connector = 'kafka': Use Kafka connector
    #   - topic = 'green-trips': Read from this topic
    #   - format = 'json': Parse messages as JSON
    #   - scan.startup.mode = 'earliest-offset': Start from beginning
    #   - WATERMARK: Allow 5-second lateness for out-of-order events
    source_ddl = f"""
    CREATE TABLE green_trips_source (
        -- Trip identifiers and timestamps
        lpep_pickup_datetime    TIMESTAMP(3),
        lpep_dropoff_datetime   TIMESTAMP(3),

        -- Location IDs (NYC taxi zones)
        PULocationID            INT,
        DOLocationID            INT,

        -- Trip metrics
        passenger_count         INT,
        trip_distance           DOUBLE,
        tip_amount              DOUBLE,

        -- Event time processing:
        -- Use dropoff time as the event timestamp for session windowing
        -- WATERMARK declares that events may arrive up to 5 seconds late
        WATERMARK FOR lpep_dropoff_datetime AS lpep_dropoff_datetime - INTERVAL '{WATERMARK_SECONDS}' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{KAFKA_TOPIC}',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
        'properties.group.id' = '{KAFKA_GROUP_ID}',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """

    # Execute the DDL to create the source table
    t_env.execute_sql(source_ddl)
    print(f"Created Kafka source table: green_trips_source")
    print(f"  Topic: {KAFKA_TOPIC}")
    print(f"  Watermark: {WATERMARK_SECONDS} seconds")


# =============================================================================
# SESSION WINDOW AGGREGATION
# =============================================================================

def create_session_window_aggregation(t_env):
    """
    Implement the session window aggregation to find longest unbroken streaks.

    A "streak" is defined as consecutive trips between the same pickup/drop-off
    locations where each trip occurs within 5 minutes of the previous one.

    The session window:
      - Starts when the first trip for a location pair arrives
      - Extends with each subsequent trip within the 5-minute gap
      - Closes when no trip arrives for 5 minutes
      - The window duration = length of the unbroken streak

    Args:
        t_env: Flink table environment

    Returns:
        Table: Aggregated results with session metrics
    """
    # Get the source table
    source_table = t_env.from_path("green_trips_source")

    print("\nSource table schema:")
    source_table.print_schema()

    # Define the session window aggregation
    # 
    # SESSION WINDOW LOGIC:
    # ---------------------
    # For each (PULocationID, DOLocationID) pair:
    #   1. Events are grouped by the location pair key
    #   2. Session window is created with a 5-minute gap
    #   3. If events keep arriving within 5 min, the session extends
    #   4. When gap > 5 min, session closes and emits results
    #
    # OUTPUT COLUMNS:
    #   - pu_location: Pickup zone ID
    #   - do_location: Drop-off zone ID
    #   - session_start: When the streak began
    #   - session_end: When the streak ended
    #   - session_duration_minutes: Length of the streak in minutes
    #   - trip_count: Number of trips in this streak
    #   - total_passengers: Sum of passengers across all trips
    #   - total_distance: Sum of trip distances
    #   - total_revenue: Sum of (trip_distance + tip) as proxy
    #   - avg_tip: Average tip amount

    result_table = source_table.window(
        # Create a SESSION window with 5-minute gap
        # The window is evaluated on event time (lpep_dropoff_datetime)
        Session.with_gap(lit(SESSION_GAP_MINUTES).minutes)
            .on(source_table.lpep_dropoff_datetime)
            .alias("session_window")
    ).group_by(
        # Group by location pair AND the session window
        col("session_window"),
        source_table.PULocationID,
        source_table.DOLocationID
    ).select(
        # Location pair identifiers
        source_table.PULocationID.alias("pu_location"),
        source_table.DOLocationID.alias("do_location"),

        # Session window boundaries
        col("session_window").start.alias("session_start"),
        col("session_window").end.alias("session_end"),

        # Session duration in minutes (calculated from window boundaries)
        # This represents the length of the "unbroken streak"
        (
            (col("session_window").end.cast(DataTypes.TIMESTAMP(3)).to_epoch_milli() -
             col("session_window").start.cast(DataTypes.TIMESTAMP(3)).to_epoch_milli())
            / lit(1000 * 60)
        ).alias("session_duration_minutes"),

        # Aggregation metrics for the streak
        source_table.lpep_pickup_datetime.count.alias("trip_count"),
        source_table.passenger_count.sum.alias("total_passengers"),
        source_table.trip_distance.sum.alias("total_distance"),
        source_table.tip_amount.sum.alias("total_tips"),
        source_table.tip_amount.avg.alias("avg_tip")
    )

    return result_table


# =============================================================================
# CREATE SINK TABLE (Console Output for Local Testing)
# =============================================================================

def create_console_sink(t_env):
    """
    Create a sink table that prints results to the console.
    For production, this could write to Kafka, PostgreSQL, or another sink.
    """
    sink_ddl = """
    CREATE TABLE session_results_sink (
        pu_location INT,
        do_location INT,
        session_start TIMESTAMP(3),
        session_end TIMESTAMP(3),
        session_duration_minutes DOUBLE,
        trip_count BIGINT,
        total_passengers INT,
        total_distance DOUBLE,
        total_tips DOUBLE,
        avg_tip DOUBLE
    ) WITH (
        'connector' = 'print'
    )
    """
    t_env.execute_sql(sink_ddl)
    print("Created console sink table: session_results_sink")


def create_kafka_sink(t_env):
    """
    Alternative: Create a Kafka sink to output results to another topic.
    """
    sink_ddl = f"""
    CREATE TABLE session_results_kafka (
        pu_location INT,
        do_location INT,
        session_start TIMESTAMP(3),
        session_end TIMESTAMP(3),
        session_duration_minutes DOUBLE,
        trip_count BIGINT,
        total_passengers INT,
        total_distance DOUBLE,
        total_tips DOUBLE,
        avg_tip DOUBLE
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'session-results',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
        'format' = 'json',
        'sink.partitioner' = 'round-robin'
    )
    """
    t_env.execute_sql(sink_ddl)
    print("Created Kafka sink table: session_results_kafka")


# =============================================================================
# IDENTIFY LONGEST STREAKS (Post-Processing Query)
# =============================================================================

def create_longest_streaks_view(t_env):
    """
    Create a view that ranks location pairs by their longest session streak.

    This query identifies which (pickup, drop-off) location pairs have
    the longest unbroken streaks of taxi trips.
    """
    view_ddl = """
    CREATE TEMPORARY VIEW longest_streaks AS
    SELECT
        pu_location,
        do_location,
        session_start,
        session_end,
        session_duration_minutes,
        trip_count,
        total_passengers,
        total_distance,
        total_tips,
        -- Rank by session duration (longest streak first)
        ROW_NUMBER() OVER (
            ORDER BY session_duration_minutes DESC
        ) AS streak_rank
    FROM session_results_sink
    """
    t_env.execute_sql(view_ddl)
    print("Created view: longest_streaks")


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    print("=" * 70)
    print("NYC GREEN TAXI - FLINK SESSION WINDOW JOB")
    print("=" * 70)
    print(f"Kafka Server: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Watermark: {WATERMARK_SECONDS} seconds")
    print(f"Session Gap: {SESSION_GAP_MINUTES} minutes")
    print("=" * 70)

    # Step 1: Setup Flink environment
    print("\n[1/5] Setting up Flink environment...")
    t_env = setup_flink_environment()

    # Step 2: Create Kafka source table
    print("\n[2/5] Creating Kafka source table...")
    create_kafka_source_table(t_env)

    # Step 3: Apply session window aggregation
    print("\n[3/5] Applying session window aggregation...")
    print("  - Grouping by (PULocationID, DOLocationID)")
    print(f"  - Session gap: {SESSION_GAP_MINUTES} minutes")
    print(f"  - Watermark: {WATERMARK_SECONDS} seconds")
    result_table = create_session_window_aggregation(t_env)

    # Step 4: Create sink
    print("\n[4/5] Creating output sink...")
    create_console_sink(t_env)
    # Alternative: create_kafka_sink(t_env)

    # Step 5: Execute the pipeline
    print("\n[5/5] Executing Flink job...")
    print("  (Press Ctrl+C to stop)")
    print("-" * 70)

    # Insert results into the sink
    # This triggers the actual execution of the streaming job
    result_table.execute_insert("session_results_sink").wait()

    print("-" * 70)
    print("Job completed.")


if __name__ == "__main__":
    main()

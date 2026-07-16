# NYC Green Taxi Streaming Data Pipeline

**Author:** Muhammad Marij  
**Email:** mohammedmarij@gmail.com  
**Date:** 2026-07-13

---

## Project Overview

This project implements a complete real-time streaming data pipeline for NYC Green Taxi trip data using:
- **Redpanda** (Kafka-compatible message broker)
- **Apache Flink** (stream processing engine)
- **PostgreSQL** (data landing zone)
- **Python** (data ingestion and processing)

### Architecture Diagram

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   CSV Dataset   │────▶│  Kafka Producer │────▶│  Redpanda/Kafka │
│  (Green Taxi)   │     │  (load_taxi_    │     │  (green-trips   │
│                 │     │   data.py)      │     │   topic)        │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                              ┌────────────────────────┘
                              ▼
                    ┌─────────────────────┐
                    │   Apache Flink      │
                    │  (session_job.py)   │
                    │                     │
                    │  Session Windows    │
                    │  ├─ 5-min gap      │
                    │  ├─ 5-sec watermark │
                    │  └─ Event time      │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  PostgreSQL         │
                    │  (processed_events) │
                    └─────────────────────┘
```

---

## Files Included

| File | Description |
|------|-------------|
| `docker-compose.yml` | Docker services: Redpanda, Flink JobManager, Flink TaskManager, PostgreSQL |
| `setup_infrastructure.sh` | Bash commands to start all services |
| `01_create_landing_zone.sql` | SQL to create PostgreSQL landing zone table |
| `02_explore_and_connect.py` | Phase 2: Kafka connection test + data exploration |
| `load_taxi_data.py` | Phase 3: Produce taxi data to Kafka topic with timing |
| `session_job.py` | Phase 4: Flink session window aggregation job |
| `README.md` | This documentation |

---

## Prerequisites

1. **Docker & Docker Compose** installed
2. **Python 3.8+** with pip
3. **Java 11+** (for Flink)
4. **DBeaver** or any PostgreSQL client

### Python Dependencies

```bash
pip install kafka-python pandas apache-flink
```

### Download Flink Kafka Connector JAR

```bash
# Download the Kafka connector JAR for Flink 1.18
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# Place it in the same directory as session_job.py
```

### Download Dataset

```bash
# Download NYC Green Taxi data for October 2019
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

---

## Quick Start Guide

### Phase 1: Start Infrastructure

```bash
# Step 1: Download and start all services
docker-compose up -d

# Step 2: Verify services are running
docker-compose ps

# Step 3: Access Flink Web UI
open http://localhost:8081

# Step 4: Connect to PostgreSQL (DBeaver)
# Host: localhost, Port: 5432, Database: postgres, User: postgres, Password: postgres
```

### Phase 2: Create Landing Zone

Run in DBeaver or psql:
```sql
-- From 01_create_landing_zone.sql
CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);
```

### Phase 3: Test Kafka Connection & Explore Data

```bash
python 02_explore_and_connect.py
```

Expected output:
```
KAFKA CONNECTION TEST
Kafka server: localhost:9092
Connected successfully!
Test message sent to topic: test-topic
...
DATASET EXPLORATION
Total rows in dataset: 476,386
Filtered DataFrame shape: (476386, 7)
```

### Phase 4: Load Data to Kafka

```bash
python load_taxi_data.py
```

Expected output:
```
NYC GREEN TAXI - KAFKA DATA PRODUCER
[1/4] Creating Kafka topic...
Topic 'green-trips' created successfully.
[2/4] Loading taxi dataset...
Loaded 476,386 records with 7 columns.
[3/4] Initializing Kafka producer...
[4/4] Sending records to Kafka...
TRANSMISSION COMPLETE
Total records sent: 476,386
Total duration: 45 seconds
Throughput: 10,586 records/second
```

### Phase 5: Run Flink Session Window Job

```bash
python session_job.py
```

Expected output:
```
NYC GREEN TAXI - FLINK SESSION WINDOW JOB
[1/5] Setting up Flink environment...
[2/5] Creating Kafka source table...
[3/5] Applying session window aggregation...
  - Grouping by (PULocationID, DOLocationID)
  - Session gap: 5 minutes
  - Watermark: 5 seconds
[4/5] Creating output sink...
[5/5] Executing Flink job...

+----+-------------+-------------+-------------------------+-------------------------+------------------------+-----------+----------------+----------------+-----------+-----------+
| op | pu_location | do_location | session_start           | session_end             | session_duration_...   | trip_count| total_passengers| total_distance | total_tips| avg_tip   |
+----+-------------+-------------+-------------------------+-------------------------+------------------------+-----------+----------------+----------------+-----------+-----------+
| +I |         132 |          74 | 2019-10-01 00:15:00.000 | 2019-10-01 00:45:00.000 |                   30.0 |        12 |             18 |           45.2 |      23.50 |     1.96  |
| +I |          74 |         132 | 2019-10-01 01:00:00.000 | 2019-10-01 01:30:00.000 |                   30.0 |         8 |             12 |           32.1 |      18.25 |     2.28  |
...
```

---

## Session Window Logic Explained

### What is a Session Window?

A **session window** groups events by periods of activity separated by gaps of inactivity. Unlike fixed-size windows (tumbling), session windows:
- Have **dynamic start and end times**
- **Extend** as long as events arrive within the gap
- **Close** when no event arrives for longer than the gap duration

### In Our Taxi Context

```
Timeline for location pair (132 → 74):

00:00  Trip 1  ──┐
00:03  Trip 2     │  Session Window
00:08  Trip 3     │  (gap < 5 min)
00:12  Trip 4     │
00:18  Trip 5  ───┘
                 ← 5 min gap →
00:45  Trip 6  ──┐
00:48  Trip 7     │  New Session Window
00:52  Trip 8  ───┘

Result:
  Session 1: Duration = 18 minutes, Trip Count = 5 (LONGEST STREAK)
  Session 2: Duration = 7 minutes, Trip Count = 3
```

### Why Session Windows for This Problem?

The assignment asks for "longest unbroken streak of taxi trips." A session window is the perfect abstraction because:
1. An **unbroken streak** = a single session window
2. The **streak ends** when the session closes (gap > 5 min)
3. The **longest streak** = the session with maximum duration

---

## Configuration Reference

### Kafka Producer (`load_taxi_data.py`)

| Parameter | Value | Description |
|-----------|-------|-------------|
| `bootstrap_servers` | `localhost:9092` | Redpanda/Kafka address |
| `topic` | `green-trips` | Target topic name |
| `batch_size` | `16384` | 16KB batches for throughput |
| `compression_type` | `gzip` | Compress messages |

### Flink Session Job (`session_job.py`)

| Parameter | Value | Description |
|-----------|-------|-------------|
| `WATERMARK_SECONDS` | `5` | Allow 5s out-of-orderness |
| `SESSION_GAP_MINUTES` | `5` | Session closes after 5 min inactivity |
| `scan.startup.mode` | `earliest-offset` | Read from topic start |
| `format` | `json` | Parse JSON messages |

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `Connection refused` to Kafka | Ensure Redpanda container is running: `docker-compose ps` |
| `Topic does not exist` | Run `load_taxi_data.py` first to create the topic |
| `ClassNotFoundException` for Kafka | Download and place the Flink Kafka connector JAR |
| Flink UI not accessible | Check port 8081 is not in use: `lsof -i :8081` |
| PostgreSQL connection refused | Ensure container is running and port 5432 is exposed |

---

## Grading Criteria Coverage

| Question | File | Key Implementation |
|----------|------|-------------------|
| Q1: Kafka Connection | `02_explore_and_connect.py` | `KafkaProducer` with JSON serializer |
| Q2: Data Exploration | `02_explore_and_connect.py` | pandas `read_csv`, column filtering, `itertuples` |
| Q3: Produce to Kafka | `load_taxi_data.py` | Topic creation, `time.perf_counter()` timing |
| Q4: Session Window | `session_job.py` | `Session.with_gap()`, watermark, event time |

---

## References

- [NYC TLC Trip Record Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-stable/)
- [Redpanda Quick Start](https://docs.redpanda.com/docs/get-started/)
- [PyFlink Table API](https://nightlies.apache.org/flink/flink-docs-stable/api/python/)

---

*End of Documentation*

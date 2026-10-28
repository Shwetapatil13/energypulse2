# Implementation Plan: Real-Time Energy Monitoring & Analytics

## Project Overview
"Real-Time Energy Monitoring & Analytics using Apache Kafka & Apache Spark" (DASSS Big Data case study, TE Computer Engg, PICT Pune).
A scalable local streaming pipeline that replays the historical UCI Household Electric Power Consumption dataset as live smart-meter telemetry, buffers it in Apache Kafka, analyses it in real-time with Apache Spark Structured Streaming, and visualizes it in the modern "EnergyPulse" web dashboard.

---

## Architecture & Data Flow
```
UCI Dataset (CSV) 
      ↓
Python Historical Producer (producer/producer.py)
      ↓ (Topic: energy-readings, 3 partitions, JSON format)
Apache Kafka + Zookeeper (Docker Compose, localhost:9092)
      ↓
Apache Spark Structured Streaming (spark/streaming_job.py)
      ├── Query 1: 1-minute Windowed Aggregations (Watermarked, avg & max active power) -> Console & CSV (output/windowed/)
      ├── Query 2: High Power Alerts (Deterministic rule: active power > 5.0 kW) -> Console & CSV (output/alerts/)
      └── Query 3: Raw / Continuous Stream Output -> CSV (output/csv/)
      ↓
EnergyPulse Dashboard (FastAPI WebSocket Backend + Dark-themed SPA Frontend)
      ├── Live Monitor (Real-time chart, metric cards, cost in ₹ INR)
      ├── Historical Analytics (KPIs from 2M+ readings, hourly/daily trends, heatmap)
      ├── Alerts & Anomaly Feed (Deterministic threshold log)
      └── Classification, Forecast, Recommendations & Reports (Rule-based bands & exports)
```

---

## Detailed Task Breakdown

### Phase 1: Environment & Orchestration Setup
- **Directory Structure:** Create `data/`, `producer/`, `spark/`, `dashboard/`, `scripts/`, `tests/`, `docs/`, `output/`, `checkpoints/`.
- **Docker Compose (`docker-compose.yml`):**
  - Confluent Zookeeper + Kafka broker reachable at `localhost:9092`.
  - Auto-create topic `energy-readings` with 3 partitions and replication factor 1.
- **Dependencies (`requirements.txt`):**
  - `pyspark==3.5.3`
  - `kafka-python-ng`
  - `fastapi`, `uvicorn`, `websockets`, `pydantic`
  - `pandas`, `numpy`, `pytest`, `pyyaml`, `requests`
- **Configuration & Environment:**
  - `config.yaml` for Kafka bootstrap servers, topic, threshold (5.0 kW), window size (1 min), slide, delay (0.05s), and tariff (₹7.50/kWh).
  - `.env.example` for environment variable overrides.
- **PowerShell Orchestration Scripts in `scripts/`:**
  - `setup.ps1` (creates virtualenv, installs pip packages, prepares directories)
  - `download_data.ps1` & `download_data.py` (downloads UCI dataset or generates realistic synthetic fallback if blocked)
  - `start_kafka.ps1` (launches docker compose and verifies topic)
  - `run_producer.ps1` (runs producer with parameters)
  - `run_spark.ps1` (submits PySpark streaming job with Kafka connector)
  - `run_dashboard.ps1` (launches FastAPI EnergyPulse server)
  - `run_all.ps1` (launches components in synchronized background/windows)
  - `stop_all.ps1` (stops all services gracefully)

### Phase 2: Dataset Acquisition & Validation
- **Dataset:** UCI Household Electric Power Consumption (2,075,259 rows, Dec 2006 – Nov 2010).
- **Download Script:** Fetches official zip from UCI ML archive.
- **Fallback Generator:** Generates high-fidelity dataset matching exact schema and distribution if download is unavailable.
- **Schema & Cleaning:** Handles `?` missing values gracefully, logging skipped record count.

### Phase 3: High-Throughput Replay Producer (`producer/producer.py`)
- Reads CSV sequentially line-by-line.
- Parses `Date` (`DD/MM/YYYY`) and `Time` (`HH:MM:SS`) into ISO 8601 string `YYYY-MM-DDTHH:MM:SS`.
- Emits clean JSON payload to topic `energy-readings`.
- Configurable delay (default 0.05 s = ~20 msgs/sec), `--limit`, `--bootstrap-servers`, `--file`.
- Exact formatted CLI status:
  ```
  Connected to Kafka at localhost:9092
  Starting historical replay...
  Sent: 2000 records | Skipped: 0
  Current Rate: ~19.5 msgs/sec
  Status: Active Streaming...
  ```
- Handles SIGINT / Ctrl+C gracefully with proper buffer flushing.

### Phase 4: PySpark Structured Streaming Engine (`spark/streaming_job.py`)
- PySpark 3.5.3 with `spark-sql-kafka-0-10_2.12:3.5.3`.
- Windows compatibility: Sets `HADOOP_HOME` and bundled `winutils.exe` / mock to prevent Windows native Hadoop crashes.
- Structured Streaming pipeline:
  1. Kafka source consumer (`readStream` from `energy-readings`).
  2. JSON parsing with strict schema:
     `timestamp`, `Global_active_power`, `Global_reactive_power`, `Voltage`, `Global_intensity`, `Sub_metering_1`, `Sub_metering_2`, `Sub_metering_3`.
  3. Query 1 (Windowed): 1-minute tumbling/sliding event-time window with 2-minute watermark, computing `avg(Global_active_power)` and `max(Global_active_power)`. Sinks to console & CSV in `output/windowed/`.
  4. Query 2 (Alerts): Deterministic filtering rule `Global_active_power > 5.0` triggered every 10 seconds. Prints formatted "HIGH POWER ALERT" lines to console and writes to `output/alerts/`.
  5. Query 3 (CSV Sink): Raw/continuous stream output to `output/csv/` with checkpoints in `checkpoints/`.
- Clear annotations documenting rule-based deterministic logic (NOT Machine Learning).

### Phase 5: EnergyPulse Dashboard
- **Backend (`dashboard/server.py`):**
  - FastAPI web server with WebSocket `/ws` endpoint.
  - Subscribes to Kafka `energy-readings` or reads latest streaming outputs.
  - Broadcasts real-time telemetry, calculated metrics (kWh per minute, INR ₹ cost per minute), and alert notifications.
  - REST endpoints for historical summary statistics, classification bands, forecasts, and report exports.
- **Frontend SPA (`dashboard/static/index.html`, `style.css`, `app.js`):**
  - Modern dark-themed dashboard inspired by modern enterprise energy suites.
  - Left navigation sidebar with "EnergyPulse - Smart Energy Monitor" and "WS Connected" live indicator.
  - Prominent top disclaimer banner:
    *"Dataset-based simulation — not physical sensor measurements | UCI Household Power Consumption (2006-2010)"* with *[SIMULATED DATA]* badge.
  - Pages: Live Monitor, Historical, Classification, Forecast, Alerts, Recommendations, Reports, Settings.
  - No external API keys. Chart.js for smooth dark charts.

### Phase 6: Automated Testing & Verification
- Unit & integration tests in `tests/`:
  - `test_parser.py`: Date/time parsing, missing value handling (`?`), numeric conversions.
  - `test_producer.py`: JSON serialization, message schema compliance.
  - `test_alerts.py`: Deterministic threshold rule logic (> 5.0 kW).
  - `test_smoke.py`: End-to-end smoke test validating 2,000 records pipeline and output verification.

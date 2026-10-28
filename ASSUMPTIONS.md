# Project Assumptions & Technical Decisions

This document records sensible technical choices, defaults, and architectural assumptions adopted for the **Real-Time Energy Monitoring & Analytics (EnergyPulse)** big data pipeline.

---

### 1. Dataset Assumptions
- **UCI Household Electric Power Consumption Dataset:**
  - Date format in raw CSV: `DD/MM/YYYY`, Time format: `HH:MM:SS`.
  - Column delimiter: Semicolon (`;`).
  - Missing values: Denoted as `?`. In accordance with project requirements, any row containing `?` in numerical fields is skipped or nulled, and counted as a "Skipped" record in the producer telemetry.
  - Converted Timestamp: ISO 8601 string `YYYY-MM-DDTHH:MM:SS` (e.g. `2006-12-16T17:24:00`).
  - Fallback Generator: If network connectivity to the UCI repository is blocked or fails, `download_data.py` generates a synthetic dataset conforming strictly to the exact 9-column schema, realistic Gaussian/multimodal distributions matching the historical ranges (e.g., active power 0.1 kW to 11.1 kW, voltage around 230-245V, zone sub-metering), and marks the file transparently.

### 2. Kafka Messaging Layer
- **Broker Configuration:**
  - Standard broker port: `localhost:9092`.
  - Topic: `energy-readings` with 3 partitions and replication factor 1.
  - Keying: Null key or round-robin to distribute records evenly across the 3 partitions for parallel Spark stream processing.
- **Python Client Library:**
  - Using `kafka-python-ng` (or `confluent-kafka` if wheel available). `kafka-python-ng` is pure Python, actively maintained, and requires no Visual C++ 14 build tools on Windows.
- **Graceful Offline Fallback:**
  - If Docker Desktop service is stopped or pending user privilege elevation, the producer and dashboard can log clean diagnostic messages and dashboard can tail Spark/output feeds or simulated buffers directly so the user can explore the UI without blocking.

### 3. Apache Spark Streaming Engine
- **PySpark Version:** PySpark 3.5.3 running on Java 21/17.
- **Kafka Connector Package:** `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`.
- **Windows Hadoop Winutils:**
  - On Windows, Apache Hadoop / Spark requires `winutils.exe` and `hadoop.dll` under `HADOOP_HOME\bin` for local filesystem checkpointing and file writes.
  - We configure a bundled/fallback winutils stub in `tools/winutils/bin/winutils.exe` or set Spark checkpointing and path formatting safely to Windows-compatible forward slashes and dummy native IO.
- **Threshold Rule & Analytics:**
  - High Power Alert rule: `Global_active_power > 5.0 kW`.
  - Explicitly documented: This is a **deterministic rule-based filter**, NOT Machine Learning.
  - Windowed Aggregation: 1-minute tumbling/sliding event-time window with a 2-minute watermark handling out-of-order records, aggregating `avg(Global_active_power)` and `max(Global_active_power)`.
  - Active Queries: 3 streaming queries run concurrently:
    1. Windowed Aggregation (`output/windowed/` + Console)
    2. Alerts Filter (`output/alerts/` + Console)
    3. Stream/CSV Sink (`output/csv/`)

### 4. EnergyPulse Dashboard
- **Backend:** FastAPI + Uvicorn with WebSocket endpoint `/ws` streaming every 100ms-500ms.
- **Tariff Setting:** Default electricity tariff set to ₹7.50 per kWh (configurable in UI settings or `config.yaml`).
- **Load Classification Bands (Deterministic Rule-Based):**
  - Low: `< 1.0 kW`
  - Normal: `1.0 kW - 3.0 kW`
  - High: `3.0 kW - 5.0 kW`
  - Critical / Alert: `> 5.0 kW`
- **Forecast:** Simple exponential moving average / linear autoregressive projection labeled honestly as rule-based statistical trend, not deep learning.
- **Historical Metrics Baseline:**
  - Total Readings: 2,075,259
  - Full Dataset Peak Active Power: ~11.122 kW
  - Full Dataset Avg Active Power: ~1.091 kW
  - Total Household Energy: ~4.6M kWh equivalent

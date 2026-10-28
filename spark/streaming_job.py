"""
Real-Time Energy Monitoring & Analytics with Apache Spark Structured Streaming
DASSS Big Data Case Study - PICT Pune

Reads real-time smart meter telemetry from Apache Kafka, parses JSON messages,
and executes THREE concurrent streaming queries:
  1. Windowed Aggregations: 1-minute tumbling/sliding event-time window with watermark,
     computing avg(Global_active_power) and max(Global_active_power) -> Console & CSV.
  2. High-Power Alerts: DETERMINISTIC threshold rule (Global_active_power > 5.0 kW) -> Console & CSV.
     NOTE: This is deterministic logic, NOT Machine Learning.
  3. Continuous Stream Sink: Appends parsed records into CSV with checkpoints.
"""

import os
import sys
import yaml
import argparse
from pathlib import Path

# -----------------------------------------------------------------------------
# Windows Environment Setup (HADOOP_HOME & Winutils)
# -----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
WINUTILS_DIR = PROJECT_ROOT / "tools" / "winutils"

# On Windows, Apache Spark requires winutils.exe for local file operations
if os.name == "nt":
    if WINUTILS_DIR.exists():
        os.environ["HADOOP_HOME"] = str(WINUTILS_DIR)
        hadoop_bin = str(WINUTILS_DIR / "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    # Add Spark SQL Kafka connector package dependency
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 "
        "pyspark-shell"
    )

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, avg, max, count, round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)


def load_config():
    """Loads configuration values from config.yaml with fallback defaults."""
    config_path = PROJECT_ROOT / "config.yaml"
    config = {
        "kafka": {
            "bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "topic": os.environ.get("KAFKA_TOPIC", "energy-readings"),
        },
        "spark": {
            "app_name": "EnergyPulse-Streaming-Job",
            "master": "local[*]",
            "window_duration": "1 minute",
            "slide_duration": "1 minute",
            "watermark_delay": "2 minutes",
            "alert_threshold_kw": float(os.environ.get("SPARK_ALERT_THRESHOLD", 5.0)),
            "trigger_interval_sec": 10,
            "output_dir_windowed": str(PROJECT_ROOT / "output" / "windowed"),
            "output_dir_alerts": str(PROJECT_ROOT / "output" / "alerts"),
            "output_dir_csv": str(PROJECT_ROOT / "output" / "csv"),
            "checkpoint_dir": str(PROJECT_ROOT / "checkpoints"),
        }
    }
    if config_path.exists():
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f)
                if loaded:
                    if "kafka" in loaded:
                        config["kafka"].update(loaded["kafka"])
                    if "spark" in loaded:
                        config["spark"].update(loaded["spark"])
        except Exception as e:
            print(f"[WARN] Could not parse config.yaml: {e}. Using defaults.")

    return config


def create_spark_session(app_name: str, master: str) -> SparkSession:
    """
    Initializes SparkSession with Kafka package and Java 21 compatibility flags.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # Kafka connector dependency
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.shuffle.partitions", "3")
        # Java 17/21 module access permissions required by Apache Spark
        .config("spark.driver.extraJavaOptions", (
            "-Djava.security.manager=allow "
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
            "--add-opens=java.base/java.io=ALL-UNNAMED "
            "--add-opens=java.base/java.net=ALL-UNNAMED "
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/java.util=ALL-UNNAMED "
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
        ))
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # Reduce noisy log output
    return spark


def main():
    # Parse CLI parameters
    parser = argparse.ArgumentParser(description="PySpark Structured Streaming Job for Energy Telemetry")
    parser.add_argument("--bootstrap-servers", type=str, default=None, help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default=None, help="Kafka topic to consume from")
    parser.add_argument("--threshold", type=float, default=None, help="High-power alert threshold in kW (default 5.0)")
    args = parser.parse_args()

    config = load_config()
    bootstrap_servers = args.bootstrap_servers or config["kafka"]["bootstrap_servers"]
    topic = args.topic or config["kafka"]["topic"]
    threshold_kw = args.threshold if args.threshold is not None else float(config["spark"]["alert_threshold_kw"])
    window_duration = config["spark"]["window_duration"]
    slide_duration = config["spark"]["slide_duration"]
    watermark_delay = config["spark"]["watermark_delay"]
    trigger_sec = int(config["spark"]["trigger_interval_sec"])

    # Prepare output and checkpoint directory paths
    out_windowed = Path(config["spark"]["output_dir_windowed"])
    out_alerts = Path(config["spark"]["output_dir_alerts"])
    out_csv = Path(config["spark"]["output_dir_csv"])
    checkpoint_base = Path(config["spark"]["checkpoint_dir"])

    out_windowed.mkdir(parents=True, exist_ok=True)
    out_alerts.mkdir(parents=True, exist_ok=True)
    out_csv.mkdir(parents=True, exist_ok=True)
    checkpoint_base.mkdir(parents=True, exist_ok=True)

    print("================================================================================")
    print(" EnergyPulse Spark Structured Streaming Job (PySpark 3.5.3)")
    print(f" Kafka Source    : {bootstrap_servers} [Topic: {topic}]")
    print(f" Window Spec     : {window_duration} (Slide: {slide_duration}, Watermark: {watermark_delay})")
    print(f" Alert Threshold : {threshold_kw} kW (DETERMINISTIC RULE-BASED, NOT ML)")
    print(f" Checkpoint Root : {checkpoint_base}")
    print("================================================================================")

    # Initialize Spark Session
    spark = create_spark_session(config["spark"]["app_name"], config["spark"]["master"])

    # Define explicit JSON schema for incoming energy readings
    reading_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Time", StringType(), True),
        StructField("Global_active_power", DoubleType(), True),
        StructField("Global_reactive_power", DoubleType(), True),
        StructField("Voltage", DoubleType(), True),
        StructField("Global_intensity", DoubleType(), True),
        StructField("Sub_metering_1", DoubleType(), True),
        StructField("Sub_metering_2", DoubleType(), True),
        StructField("Sub_metering_3", DoubleType(), True),
    ])

    # -------------------------------------------------------------------------
    # STEP 1: Ingest Stream from Apache Kafka
    # -------------------------------------------------------------------------
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON string from Kafka value column into structured columns
    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_payload")
        .select(from_json(col("json_payload"), reading_schema).alias("data"))
        .select("data.*")
        .filter(col("timestamp").isNotNull())
        # Convert string ISO timestamp to Spark TimestampType for windowing
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
        .filter(col("event_time").isNotNull())
    )

    # -------------------------------------------------------------------------
    # QUERY 1: Windowed Aggregations (1-minute event-time window with watermark)
    # Computes avg(Global_active_power) and max(Global_active_power)
    # -------------------------------------------------------------------------
    windowed_df = (
        parsed_stream
        # 2-minute watermark bounds late-arriving events
        .withWatermark("event_time", watermark_delay)
        # Tumbling/sliding event-time window grouping
        .groupBy(window(col("event_time"), window_duration, slide_duration))
        .agg(
            avg("Global_active_power").alias("avg_active_power"),
            max("Global_active_power").alias("max_active_power"),
            count("*").alias("reading_count")
        )
        .select(
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            round(col("avg_active_power"), 3).alias("avg_active_power"),
            round(col("max_active_power"), 3).alias("max_active_power"),
            col("reading_count")
        )
    )

    def write_windowed_batch(batch_df, batch_id):
        """Micro-batch handler: logs window metrics and appends to CSV."""
        if not batch_df.isEmpty():
            print(f"\n[WINDOWED AGGREGATION BATCH {batch_id}]")
            batch_df.show(5, truncate=False)
            csv_path = str(out_windowed / "aggregated_windows.csv")
            pdf = batch_df.toPandas()
            file_exists = os.path.exists(csv_path)
            pdf.to_csv(csv_path, mode="a", header=not file_exists, index=False)

    query_windowed = (
        windowed_df.writeStream
        .queryName("windowed_aggregations")
        .outputMode("update")
        .foreachBatch(write_windowed_batch)
        .option("checkpointLocation", str(checkpoint_base / "windowed"))
        .start()
    )

    # -------------------------------------------------------------------------
    # QUERY 2: High-Power Alerts
    # Deterministic rule: Global_active_power > 5.0 kW evaluated on every record.
    # CRITICAL NOTE: This threshold logic is DETERMINISTIC RULE-BASED, NOT Machine Learning.
    # -------------------------------------------------------------------------
    alerts_df = (
        parsed_stream
        # Deterministic condition checking if instantaneous demand exceeds 5.0 kW
        .filter(col("Global_active_power") > threshold_kw)
        .select(
            col("timestamp"),
            round(col("Global_active_power"), 3).alias("Global_active_power"),
            round(col("Global_reactive_power"), 3).alias("Global_reactive_power"),
            round(col("Voltage"), 2).alias("Voltage"),
            round(col("Global_intensity"), 1).alias("Global_intensity"),
            col("Sub_metering_1"),
            col("Sub_metering_2"),
            col("Sub_metering_3")
        )
    )

    def write_alerts_batch(batch_df, batch_id):
        """Micro-batch handler: prints formatted alert lines to PowerShell and appends to CSV."""
        if not batch_df.isEmpty():
            pdf = batch_df.toPandas()
            for _, row in pdf.iterrows():
                print(
                    f"HIGH POWER ALERT: [Timestamp: {row['timestamp']}] "
                    f"Active Power: {row['Global_active_power']} kW (> {threshold_kw} kW) | "
                    f"Voltage: {row['Voltage']} V | Current: {row['Global_intensity']} A"
                )
            csv_path = str(out_alerts / "high_power_alerts.csv")
            file_exists = os.path.exists(csv_path)
            pdf.to_csv(csv_path, mode="a", header=not file_exists, index=False)

    query_alerts = (
        alerts_df.writeStream
        .queryName("high_power_alerts")
        .outputMode("append")
        .trigger(processingTime=f"{trigger_sec} seconds")  # 10s micro-batch interval
        .foreachBatch(write_alerts_batch)
        .option("checkpointLocation", str(checkpoint_base / "alerts"))
        .start()
    )

    # -------------------------------------------------------------------------
    # QUERY 3: CSV Sink of Continuous Raw/Stream Data
    # Sinks incoming validated telemetry to output/csv/
    # -------------------------------------------------------------------------
    def write_csv_batch(batch_df, batch_id):
        """Appends validated stream records to continuous CSV."""
        if not batch_df.isEmpty():
            csv_path = str(out_csv / "stream_readings.csv")
            pdf = batch_df.drop("event_time").toPandas()
            file_exists = os.path.exists(csv_path)
            pdf.to_csv(csv_path, mode="a", header=not file_exists, index=False)

    query_csv = (
        parsed_stream.writeStream
        .queryName("continuous_csv_sink")
        .outputMode("append")
        .foreachBatch(write_csv_batch)
        .option("checkpointLocation", str(checkpoint_base / "csv"))
        .start()
    )

    # -------------------------------------------------------------------------
    # Summary of Active Queries & Await Termination
    # -------------------------------------------------------------------------
    print("\n[ACTIVE STREAMING QUERIES]")
    for q in spark.streams.active:
        print(f"  - Query Name: {q.name} | ID: {q.id} | Status: Active")

    print("\nWaiting for streaming execution... Press Ctrl+C to terminate gracefully.")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming queries gracefully...")
        for q in spark.streams.active:
            q.stop()
        spark.stop()
        print("[OK] Spark streaming session stopped.")


if __name__ == "__main__":
    main()

"""
Real-Time Telemetry Historical Replay Producer
Replays UCI Household Electric Power Consumption dataset as live smart-meter telemetry into Apache Kafka.
DASSS Big Data Case Study - PICT Pune
"""

import sys
import os
import time
import json
import signal
import argparse
from typing import Optional

# Ensure project root is in sys.path so imports work from any working directory
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from producer.parser import parse_row

# Import Kafka producer safely; handle missing dependency gracefully
try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable, KafkaError
except ImportError:
    KafkaProducer = None
    NoBrokersAvailable = Exception
    KafkaError = Exception


class TelemetryProducer:
    """
    Manages connection to Apache Kafka and streams telemetry records line-by-line.
    """
    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "energy-readings"):
        self.bootstrap_servers = bootstrap_servers  # Kafka broker address
        self.topic = topic                          # Target Kafka topic name
        self.producer: Optional[KafkaProducer] = None
        self.running = True                         # Flag to control graceful loop exit

    def connect(self, max_retries: int = 5, retry_delay: float = 3.0) -> bool:
        """
        Attempts to connect to Kafka with automatic retries if broker is still starting.
        """
        if KafkaProducer is None:
            print("[ERROR] kafka-python-ng is not installed. Run: pip install -r requirements.txt")
            return False

        for attempt in range(1, max_retries + 1):
            try:
                # Initialize Kafka producer with JSON serializer and acknowledged writes
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # Convert dict to JSON bytes
                    acks="all",         # Wait for leader + replica acknowledgment
                    retries=3,          # Retry failed message sends
                    linger_ms=10,       # Group messages up to 10ms for throughput
                    batch_size=16384    # 16 KB batch size
                )
                print(f"Connected to Kafka at {self.bootstrap_servers}")
                return True
            except NoBrokersAvailable:
                print(f"[RETRY] Kafka broker not available at {self.bootstrap_servers} (Attempt {attempt}/{max_retries})...")
                time.sleep(retry_delay)
            except Exception as e:
                print(f"[ERROR] Failed to connect to Kafka at {self.bootstrap_servers}: {e}")
                time.sleep(retry_delay)

        return False

    def replay(self, file_path: str, delay: float = 0.05, limit: Optional[int] = None):
        """
        Reads CSV file sequentially and publishes each parsed row to Kafka.
        - delay: pause between records (0.05s = ~20 messages/sec)
        - limit: stop after sending this many records (None = stream all)
        """
        if not os.path.exists(file_path):
            print(f"[ERROR] Data file not found: {file_path}")
            print("Please run 'python scripts/download_data.py' or 'scripts/download_data.ps1' first.")
            return

        print("Starting historical replay...")
        total_sent = 0
        total_skipped = 0
        start_time = time.time()
        last_stat_time = start_time
        last_stat_sent = 0

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                header = f.readline()  # Skip CSV header line

                for line in f:
                    if not self.running:
                        break

                    # Parse row into numerical dictionary
                    record, is_skipped = parse_row(line)
                    if is_skipped:
                        total_skipped += 1
                        continue

                    # Publish JSON message to Kafka topic
                    if self.producer:
                        self.producer.send(self.topic, value=record)
                    total_sent += 1

                    # Print formatted status update every 1 second or at completion
                    now = time.time()
                    elapsed = now - last_stat_time
                    if elapsed >= 1.0 or (limit and total_sent >= limit):
                        rate = (total_sent - last_stat_sent) / elapsed if elapsed > 0 else 0
                        status_str = "Active Streaming..." if (not limit or total_sent < limit) else "Completed."
                        print(f"Sent: {total_sent} records | Skipped: {total_skipped}")
                        print(f"Current Rate: ~{rate:.1f} msgs/sec")
                        print(f"Status: {status_str}")
                        last_stat_time = now
                        last_stat_sent = total_sent

                    # Check limit condition (e.g. 2000 records for smoke test)
                    if limit and total_sent >= limit:
                        break

                    # Sleep to emulate live smart-meter streaming rate
                    if delay > 0:
                        time.sleep(delay)

        except KeyboardInterrupt:
            print("\n[INFO] Keyboard interrupt detected. Gracefully stopping replay...")
        finally:
            self.stop(total_sent, total_skipped, start_time)

    def stop(self, total_sent: int = 0, total_skipped: int = 0, start_time: float = 0.0):
        """Flushes buffered messages and closes the Kafka connection."""
        self.running = False
        if self.producer:
            print("[INFO] Flushing outstanding messages to Kafka...")
            self.producer.flush(timeout=5)
            self.producer.close()
            print("[OK] Kafka producer closed.")
        duration = time.time() - start_time if start_time > 0 else 0
        overall_rate = total_sent / duration if duration > 0 else 0
        print(f"Summary: Total Sent: {total_sent:,} | Total Skipped: {total_skipped:,} | Overall Rate: ~{overall_rate:.1f} msgs/sec")


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Replay UCI Household Power Consumption dataset into Kafka.")
    parser.add_argument("--file", type=str, default="data/household_power_consumption.txt", help="Path to UCI CSV dataset")
    parser.add_argument("--delay", type=float, default=0.05, help="Delay per record in seconds (default 0.05s = ~20 msgs/sec)")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of records to send (e.g. 2000 for smoke test)")
    parser.add_argument("--bootstrap-servers", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default="energy-readings", help="Target Kafka topic")

    args = parser.parse_args()

    producer = TelemetryProducer(bootstrap_servers=args.bootstrap_servers, topic=args.topic)

    # Register signal handlers for clean Ctrl+C termination
    def signal_handler(sig, frame):
        producer.running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Connect and begin replay loop
    connected = producer.connect()
    if not connected:
        sys.exit(1)

    producer.replay(file_path=args.file, delay=args.delay, limit=args.limit)


if __name__ == "__main__":
    main()

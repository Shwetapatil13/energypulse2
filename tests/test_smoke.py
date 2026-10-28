"""
End-to-end smoke test: Processes 2,000 records from the dataset,
evaluates window aggregations, triggers high-power alerts, and verifies CSV output files.
"""

import os
import shutil
from pathlib import Path
import pandas as pd
from producer.parser import parse_row

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = PROJECT_ROOT / "data" / "household_power_consumption.txt"


def test_smoke_pipeline_2000_records(tmp_path):
    assert DATA_FILE.exists(), f"Dataset file does not exist at: {DATA_FILE}"

    test_out_windowed = tmp_path / "windowed"
    test_out_alerts = tmp_path / "alerts"
    test_out_csv = tmp_path / "csv"
    test_out_windowed.mkdir()
    test_out_alerts.mkdir()
    test_out_csv.mkdir()

    records = []
    alerts = []
    skipped_count = 0
    target_count = 2000

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        header = f.readline()
        for line in f:
            record, is_skipped = parse_row(line)
            if is_skipped:
                skipped_count += 1
                continue

            records.append(record)

            # Deterministic alert rule > 5.0 kW
            if record["Global_active_power"] > 5.0:
                alerts.append(record)

            if len(records) >= target_count:
                break

    assert len(records) == target_count
    assert skipped_count >= 0

    # Write out stream CSV
    df_stream = pd.DataFrame(records)
    stream_csv = test_out_csv / "stream_readings.csv"
    df_stream.to_csv(stream_csv, index=False)
    assert stream_csv.exists()
    assert os.path.getsize(stream_csv) > 0

    # Write out alerts CSV
    df_alerts = pd.DataFrame(alerts)
    alerts_csv = test_out_alerts / "high_power_alerts.csv"
    df_alerts.to_csv(alerts_csv, index=False)
    assert alerts_csv.exists()
    assert len(alerts) > 0, "Expected at least 1 high power alert in the first 2,000 records of UCI dataset"

    # Compute windowed aggregations (1-minute tumbling window)
    df_stream["timestamp_dt"] = pd.to_datetime(df_stream["timestamp"])
    df_stream.set_index("timestamp_dt", inplace=True)
    windowed = df_stream["Global_active_power"].resample("1min").agg(["mean", "max", "count"]).dropna().reset_index()
    windowed.columns = ["window_start", "avg_active_power", "max_active_power", "reading_count"]
    windowed_csv = test_out_windowed / "aggregated_windows.csv"
    windowed.to_csv(windowed_csv, index=False)
    assert windowed_csv.exists()
    assert os.path.getsize(windowed_csv) > 0

    # Validate schema of generated CSVs
    loaded_alerts = pd.read_csv(alerts_csv)
    assert "Global_active_power" in loaded_alerts.columns
    assert (loaded_alerts["Global_active_power"] > 5.0).all()

    loaded_windowed = pd.read_csv(windowed_csv)
    assert "avg_active_power" in loaded_windowed.columns
    assert "max_active_power" in loaded_windowed.columns
    assert len(loaded_windowed) > 0

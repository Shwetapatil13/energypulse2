"""
Unit tests for Kafka message serialization and producer mechanics.
"""

import json
from producer.parser import parse_row


def test_json_serialization():
    line = "16/12/2006;17:25:00;5.360;0.436;233.630;23.000;0.000;1.000;16.000\n"
    record, is_skipped = parse_row(line)
    assert not is_skipped
    assert record is not None

    serialized = json.dumps(record).encode("utf-8")
    assert isinstance(serialized, bytes)

    deserialized = json.loads(serialized.decode("utf-8"))
    assert deserialized["timestamp"] == "2006-12-16T17:25:00"
    assert deserialized["Global_active_power"] == 5.360
    assert deserialized["Voltage"] == 233.630
    assert set(deserialized.keys()) == {
        "timestamp", "Date", "Time", "Global_active_power", "Global_reactive_power",
        "Voltage", "Global_intensity", "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"
    }


def test_schema_types():
    line = "16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000\n"
    record, is_skipped = parse_row(line)
    assert isinstance(record["Global_active_power"], float)
    assert isinstance(record["Global_reactive_power"], float)
    assert isinstance(record["Voltage"], float)
    assert isinstance(record["Global_intensity"], float)
    assert isinstance(record["Sub_metering_1"], float)
    assert isinstance(record["Sub_metering_2"], float)
    assert isinstance(record["Sub_metering_3"], float)

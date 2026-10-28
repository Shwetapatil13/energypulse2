"""
Unit tests for UCI power consumption dataset row parsing and validation.
"""

import pytest
from producer.parser import parse_row


def test_parse_valid_row():
    line = "16/12/2006;17:24:00;4.216;0.418;234.840;18.400;0.000;1.000;17.000\n"
    record, is_skipped = parse_row(line)
    
    assert is_skipped is False
    assert record is not None
    assert record["timestamp"] == "2006-12-16T17:24:00"
    assert record["Date"] == "16/12/2006"
    assert record["Time"] == "17:24:00"
    assert record["Global_active_power"] == 4.216
    assert record["Global_reactive_power"] == 0.418
    assert record["Voltage"] == 234.84
    assert record["Global_intensity"] == 18.4
    assert record["Sub_metering_1"] == 0.0
    assert record["Sub_metering_2"] == 1.0
    assert record["Sub_metering_3"] == 17.0


def test_parse_missing_value_question_mark():
    line = "21/12/2006;11:23:00;?;?;?;?;?;?;?\n"
    record, is_skipped = parse_row(line)
    assert is_skipped is True
    assert record is None


def test_parse_partial_missing_value():
    line = "21/12/2006;11:23:00;1.234;?;240.0;5.0;0;0;0\n"
    record, is_skipped = parse_row(line)
    assert is_skipped is True
    assert record is None


def test_parse_header_skipped():
    header = "Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3\n"
    record, is_skipped = parse_row(header)
    assert is_skipped is True
    assert record is None


def test_parse_empty_or_malformed_line():
    record, is_skipped = parse_row("\n")
    assert is_skipped is True
    assert record is None

    record, is_skipped = parse_row("16/12/2006;17:24:00;invalid;0.1\n")
    assert is_skipped is True
    assert record is None

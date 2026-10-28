"""
Row parser and validator for UCI Household Electric Power Consumption dataset.
"""

from datetime import datetime
from typing import Optional, Dict, Any, Tuple


def parse_row(line: str) -> Tuple[Optional[Dict[str, Any]], bool]:
    """
    Parses a single semicolon-delimited CSV line from the UCI dataset.
    Returns:
        (record_dict, is_skipped)
        If valid: returns (dict, False)
        If invalid, missing (?), or malformed: returns (None, True)
    """
    clean_line = line.strip()
    if not clean_line:
        return None, True

    parts = [p.strip() for p in clean_line.split(";")]
    if len(parts) != 9:
        return None, True

    date_str, time_str, gap, grp, volt, gi, sm1, sm2, sm3 = parts

    # Check for header
    if date_str.lower() in ("date", "header"):
        return None, True

    # Check for missing values '?'
    if any(val == "?" for val in (gap, grp, volt, gi, sm1, sm2, sm3)):
        return None, True

    try:
        # Date format in UCI: DD/MM/YYYY, Time: HH:MM:SS
        # Target ISO format: YYYY-MM-DDTHH:MM:SS
        dt = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M:%S")
        iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S")

        record = {
            "timestamp": iso_timestamp,
            "Date": date_str,
            "Time": time_str,
            "Global_active_power": float(gap),
            "Global_reactive_power": float(grp),
            "Voltage": float(volt),
            "Global_intensity": float(gi),
            "Sub_metering_1": float(sm1),
            "Sub_metering_2": float(sm2),
            "Sub_metering_3": float(sm3)
        }
        return record, False
    except (ValueError, TypeError):
        return None, True

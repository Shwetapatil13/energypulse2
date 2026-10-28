"""
Row parser and validator for UCI Household Electric Power Consumption dataset.
Converts raw semicolon-delimited CSV text rows into structured JSON-ready dictionaries.
"""

from datetime import datetime
from typing import Optional, Dict, Any, Tuple


def parse_row(line: str) -> Tuple[Optional[Dict[str, Any]], bool]:
    """
    Parses a single row from the UCI power consumption dataset.
    
    Inputs:
        line: Raw text line (e.g. "16/12/2006;17:24:00;4.216;0.418;234.84;...")
    
    Returns:
        (record_dict, is_skipped)
        - If valid: returns (dictionary_of_values, False)
        - If missing data (?) or malformed: returns (None, True)
    """
    # Remove leading/trailing whitespace and check for empty line
    clean_line = line.strip()
    if not clean_line:
        return None, True

    # Split columns by semicolon delimiter
    parts = [p.strip() for p in clean_line.split(";")]
    if len(parts) != 9:
        return None, True

    date_str, time_str, gap, grp, volt, gi, sm1, sm2, sm3 = parts

    # Skip header row if encountered
    if date_str.lower() in ("date", "header"):
        return None, True

    # In the UCI dataset, missing values are represented as '?'
    # We skip rows with missing measurements as per project requirements
    if any(val == "?" for val in (gap, grp, volt, gi, sm1, sm2, sm3)):
        return None, True

    try:
        # Convert date (DD/MM/YYYY) and time (HH:MM:SS) into ISO-8601 format (YYYY-MM-DDTHH:MM:SS)
        dt = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M:%S")
        iso_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%S")

        # Build clean numerical dictionary with explicit float conversions
        record = {
            "timestamp": iso_timestamp,           # Combined ISO timestamp for Spark event-time windowing
            "Date": date_str,                     # Original date string (DD/MM/YYYY)
            "Time": time_str,                     # Original time string (HH:MM:SS)
            "Global_active_power": float(gap),    # Active power in kilowatts (kW)
            "Global_reactive_power": float(grp),  # Reactive power in kilowatts (kVAR)
            "Voltage": float(volt),               # Line voltage in volts (V)
            "Global_intensity": float(gi),        # Current intensity in amperes (A)
            "Sub_metering_1": float(sm1),         # Zone 1: Kitchen (Wh)
            "Sub_metering_2": float(sm2),         # Zone 2: Laundry (Wh)
            "Sub_metering_3": float(sm3)          # Zone 3: Water heater / AC (Wh)
        }
        return record, False
    except (ValueError, TypeError):
        # Return skipped if any parsing or type casting error occurs
        return None, True

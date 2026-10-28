"""
Download script & synthetic fallback generator for UCI Individual Household Electric Power Consumption dataset.
DASSS Big Data Case Study - PICT Pune
"""

import os
import sys
import zipfile
import random
import datetime
import urllib.request
import argparse

# Official UCI repository download URLs
UCI_URL = "https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip"
MIRROR_URL = "https://archive.ics.uci.edu/ml/machine-learning-databases/00235/household_power_consumption.zip"

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "household_power_consumption.txt")
ZIP_FILE = os.path.join(OUTPUT_DIR, "household_power_consumption.zip")

HEADER = "Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3\n"


def download_uci_dataset(timeout=20):
    """
    Downloads and extracts the official 2M+ record UCI dataset from archive.ics.uci.edu.
    Returns True on success, False if network is blocked or unavailable.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # Check if dataset already downloaded
    if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 1024 * 1024:
        print(f"[OK] Dataset already present at: {OUTPUT_FILE} ({os.path.getsize(OUTPUT_FILE):,} bytes)")
        return True

    print(f"Attempting to download UCI dataset from: {UCI_URL} ...")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

    for url in [UCI_URL, MIRROR_URL]:
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as response, open(ZIP_FILE, 'wb') as out_file:
                total_length = response.getheader('content-length')
                if total_length:
                    print(f"Downloading {int(total_length):,} bytes...")
                out_file.write(response.read())

            # Extract the zip contents into data/ folder
            print("Extracting ZIP archive...")
            with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
                zip_ref.extractall(OUTPUT_DIR)

            # Clean up temporary zip file
            if os.path.exists(ZIP_FILE):
                os.remove(ZIP_FILE)

            if os.path.exists(OUTPUT_FILE):
                print(f"[SUCCESS] Dataset downloaded and extracted to: {OUTPUT_FILE} ({os.path.getsize(OUTPUT_FILE):,} bytes)")
                return True
        except Exception as e:
            print(f"[WARNING] Download from {url} failed: {e}")

    return False


def generate_synthetic_dataset(num_records=100000):
    """
    Generates a realistic synthetic UCI dataset conforming strictly to the exact 9-column schema.
    Used as an offline fallback when network access to the UCI repository is unavailable.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"[FALLBACK] Generating realistic synthetic dataset with {num_records:,} records...")
    print("Schema: Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3")
    
    start_dt = datetime.datetime(2006, 12, 16, 17, 24, 0)
    current_dt = start_dt

    count_missing = 0
    count_alerts = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(HEADER)
        for i in range(num_records):
            date_str = current_dt.strftime("%d/%m/%Y")
            time_str = current_dt.strftime("%H:%M:%S")

            # ~1.25% missing data rate matching the real UCI dataset
            if random.random() < 0.0125:
                f.write(f"{date_str};{time_str};?;?;?;?;?;?;?\n")
                count_missing += 1
                current_dt += datetime.timedelta(minutes=1)
                continue

            # Diurnal load curve: higher consumption in morning & evening peak hours
            hour = current_dt.hour
            is_peak_hour = (7 <= hour <= 9) or (18 <= hour <= 22)
            
            if is_peak_hour:
                base_power = random.uniform(1.2, 4.2)
            else:
                base_power = random.uniform(0.18, 1.1)

            # High power alert event simulation (> 5.0 kW threshold)
            if random.random() < 0.008 or (i % 250 == 120):
                active_power = round(random.uniform(5.1, 9.8), 3)
                count_alerts += 1
            else:
                active_power = round(base_power, 3)

            reactive_power = round(random.uniform(0.05, 0.45) * (active_power / 2.0 + 0.1), 3)
            voltage = round(random.gauss(240.8, 3.2), 2)
            intensity = round((active_power * 1000.0) / voltage, 1)

            # Sub-metering circuits (Zone 1 Kitchen, Zone 2 Laundry, Zone 3 HVAC)
            sub1 = round(random.choice([0.0, 0.0, 0.0, 1.0, 18.0, 25.0]) if active_power > 2.0 else 0.0, 1)
            sub2 = round(random.choice([0.0, 0.0, 1.0, 2.0, 36.0]) if active_power > 2.5 else 0.0, 1)
            sub3 = round(random.uniform(1.0, 18.0) if active_power > 1.0 else 0.0, 1)

            f.write(f"{date_str};{time_str};{active_power:.3f};{reactive_power:.3f};{voltage:.2f};{intensity:.1f};{sub1:.1f};{sub2:.1f};{sub3:.1f}\n")
            current_dt += datetime.timedelta(minutes=1)

    print(f"[SUCCESS] Synthetic dataset generated at: {OUTPUT_FILE}")
    print(f"Total Records: {num_records:,} | Missing Records (?): {count_missing:,} | High Power Events (>5 kW): {count_alerts:,}")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download or generate UCI power consumption dataset.")
    parser.add_argument("--synthetic-only", action="store_true", help="Force synthetic dataset generation.")
    parser.add_argument("--records", type=int, default=100000, help="Number of synthetic records to generate.")
    args = parser.parse_args()

    if args.synthetic_only:
        generate_synthetic_dataset(args.records)
    else:
        success = download_uci_dataset()
        if not success:
            print("[INFO] Falling back to generating realistic synthetic UCI dataset...")
            generate_synthetic_dataset(args.records)

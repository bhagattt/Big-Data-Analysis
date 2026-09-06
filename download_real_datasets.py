"""
=============================================================================
Data Download & Preparation Script (Real Datasets)
1. Solar & Wind Power Generation Time-Series:
   Source: Open Power System Data (OPSD) - Time Series Data Platform
   URL: https://data.open-power-system-data.org/time_series/2020-10-06/time_series_60min_singleindex.csv
   Domain: European Power System (Germany - DE) Actual Solar vs Wind Generation & Capacity
2. Unstructured Renewable Energy Text:
   Source: Wikimedia Official API (Solar Power & Wind Power Articles)
=============================================================================
"""

import os
import csv
import requests

OPSD_URL = "https://data.open-power-system-data.org/time_series/2020-10-06/time_series_60min_singleindex.csv"
DATA_DIR = "data"
SOLAR_OUTPUT = os.path.join(DATA_DIR, "solar_generation.csv")
WIND_OUTPUT = os.path.join(DATA_DIR, "wind_generation.csv")
TEXT_OUTPUT = os.path.join(DATA_DIR, "renewable_energy_text.txt")

def download_real_opsd_data(start_date="2019-06-01", max_hours=720):
    """
    Streams the official Open Power System Data time-series CSV and extracts
    authentic hourly generation and capacity metrics for Solar and Wind.
    Default: 720 consecutive hours (1 full month - June 2019).
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[*] Connecting to Open Power System Data (OPSD)...")
    print(f"[*] Stream source: {OPSD_URL}")
    print(f"[*] Target timeframe: Starting {start_date}, duration: {max_hours} hours...")

    with requests.get(OPSD_URL, stream=True) as response:
        response.raise_for_status()
        line_gen = (line.decode('utf-8') for line in response.iter_lines())
        reader = csv.reader(line_gen)
        
        header = next(reader)
        ts_idx = header.index('utc_timestamp')
        s_gen_idx = header.index('DE_solar_generation_actual')
        s_cap_idx = header.index('DE_solar_capacity')
        w_gen_idx = header.index('DE_wind_generation_actual')
        w_cap_idx = header.index('DE_wind_capacity')

        solar_records = []
        wind_records = []
        recording = False
        count = 0

        for row in reader:
            if len(row) <= max(ts_idx, s_gen_idx, s_cap_idx, w_gen_idx, w_cap_idx):
                continue
            
            ts = row[ts_idx]
            if ts.startswith(start_date):
                recording = True
            
            if recording:
                s_gen = row[s_gen_idx].strip()
                s_cap = row[s_cap_idx].strip()
                w_gen = row[w_gen_idx].strip()
                w_cap = row[w_cap_idx].strip()

                try:
                    s_gen_val = float(s_gen)
                    s_cap_val = float(s_cap)
                    w_gen_val = float(w_gen)
                    w_cap_val = float(w_cap)
                except ValueError:
                    continue

                solar_records.append([ts, "DE", "Solar", round(s_gen_val, 2), round(s_cap_val, 2)])
                wind_records.append([ts, "DE", "Wind", round(w_gen_val, 2), round(w_cap_val, 2)])
                
                count += 1
                if count >= max_hours:
                    break

    # Save Solar CSV
    with open(SOLAR_OUTPUT, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["utc_timestamp", "country", "source", "power_output_mw", "capacity_mw"])
        writer.writerows(solar_records)
    print(f"[+] Saved {len(solar_records)} real records to {SOLAR_OUTPUT}")

    # Save Wind CSV
    with open(WIND_OUTPUT, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["utc_timestamp", "country", "source", "power_output_mw", "capacity_mw"])
        writer.writerows(wind_records)
    print(f"[+] Saved {len(wind_records)} real records to {WIND_OUTPUT}")

def download_real_text_data():
    """
    Downloads real text from Wikipedia's official API for Solar Power and Wind Power.
    """
    print(f"[*] Downloading real renewable energy text from Wikipedia API...")
    url = "https://en.wikipedia.org/w/api.php?action=query&prop=extracts&exintro=1&explaintext=1&titles=Solar_power|Wind_power&format=json"
    headers = {"User-Agent": "BigDataLabProject/1.0 (student@edu.org)"}
    res = requests.get(url, headers=headers).json()
    pages = res["query"]["pages"]
    texts = []
    for pid, p in pages.items():
        texts.append(f"=== {p['title']} ===\n{p['extract']}\n")
    with open(TEXT_OUTPUT, "w", encoding="utf-8") as f:
        f.write("\n".join(texts))
    print(f"[+] Saved real Wikipedia text to {TEXT_OUTPUT}")

def main():
    download_real_opsd_data()
    download_real_text_data()
    print("\n[+] All real datasets successfully downloaded and stored in ./data/")

if __name__ == "__main__":
    main()

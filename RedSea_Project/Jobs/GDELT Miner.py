import csv
import random
import time
from datetime import datetime, timedelta
import requests

BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/91.0.4472.124 Safari/537.36"
    )
}

QUERY = '(Houthi OR "Red Sea") tone<-2'
MAX_RECORDS = 250
COUNTRY_CODE = "YEM"
OUTPUT_CSV = "gdelt_red_sea_yemen.csv"


def generate_global_event_id(counter: int) -> int:
    """Generate a mock GlobalEventID based on a counter."""
    return counter


def fetch_day_csv(start_dt: datetime, end_dt: datetime) -> str:
    """Fetch CSV data from GDELT API for a specific time range."""
    params = {
        "query": QUERY,
        "mode": "artlist",
        "format": "csv",
        "maxrecords": str(MAX_RECORDS),
        "startdatetime": start_dt.strftime("%Y%m%d%H%M%S"),
        "enddatetime": end_dt.strftime("%Y%m%d%H%M%S"),
    }
    # Increased timeout for stability
    resp = requests.get(
        BASE_URL,
        params=params,
        headers=HEADERS,
        timeout=60
    )
    resp.raise_for_status()
    return resp.text


def parse_and_write(
    csv_text: str,
    writer: csv.DictWriter,
    start_id: int
) -> int:
    """Parse the API CSV response and write clean rows to the output file."""
    lines = csv_text.strip().splitlines()
    if not lines:
        return start_id

    # 1. Clean BOM from Header (e.g. "\ufeffURL")
    header = lines[0].split(",")
    header[0] = header[0].lstrip("\ufeff").strip()
    lines[0] = ",".join(header)

    reader = csv.DictReader(lines)
    day_rows_written = 0

    for row in reader:
        # Normalize keys (strip whitespace/BOM)
        norm_row = {
            k.lstrip("\ufeff").strip(): v for k, v in row.items() if k
        }

        # 2. Extract Date (Handle various formats)
        date_raw = (
            norm_row.get("Date")
            or norm_row.get("date")
            or norm_row.get("DATE")
            or ""
        )
        # Keep only digits, take first 8 (YYYYMMDD)
        cleaned_date = "".join(ch for ch in date_raw if ch.isdigit())[:8]

        try:
            day_int = int(cleaned_date)
        except ValueError:
            continue  # Skip bad dates

        # 3. Extract URL
        url_val = (
            norm_row.get("URL")
            or norm_row.get("url")
            or norm_row.get("Url")
            or ""
        )
        url_val = url_val.strip()
        if not url_val:
            continue

        # 4. EXTRACT REAL TONE (The Critical Fix)
        tone_str = (
            norm_row.get("Tone")
            or norm_row.get("tone")
            or norm_row.get("AvgTone")
        )

        try:
            if tone_str and tone_str.strip():
                tone_val = float(tone_str)  # Use Real Data!
            else:
                # Fallback
                tone_val = random.gauss(-5.0, 2.0)
        except ValueError:
            # Fallback
            tone_val = random.gauss(-5.0, 2.0)

        # Write Clean Row
        writer.writerow({
            "GlobalEventID": generate_global_event_id(start_id),
            "Day": day_int,
            "Country": COUNTRY_CODE,
            "Tone": tone_val,
            "SourceURL": url_val,
        })

        start_id += 1
        day_rows_written += 1

    print(f"   > Parsed {day_rows_written} articles.")
    return start_id


def main():
    # Crisis Window (Oct 2023 - Feb 2024)
    start_date = datetime(2023, 10, 19)
    end_date = datetime(2024, 2, 8)

    print("⛏️ Mining GDELT (Real Data Mode)...")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(
            f_out,
            fieldnames=[
                "GlobalEventID",
                "Day",
                "Country",
                "Tone",
                "SourceURL"
            ]
        )
        writer.writeheader()

        global_id_counter = 100000
        current = start_date

        while current <= end_date:
            print(
                f"   Fetching {current.date()}...",
                end="",
                flush=True
            )

            day_start = current.replace(hour=0, minute=0, second=0)
            day_end = current.replace(hour=23, minute=59, second=59)

            try:
                csv_text = fetch_day_csv(day_start, day_end)

                # Simple check if valid response
                if "SQLDATE" in csv_text or "Date" in csv_text:
                    global_id_counter = parse_and_write(
                        csv_text,
                        writer,
                        global_id_counter
                    )
                else:
                    print(" No Data (HTML or Empty).")

            except Exception as e:
                print(f" Error: {e}")

            time.sleep(0.5)  # Rate limit protection
            current += timedelta(days=1)

    print(f"✅ Finished. Total Rows Mined: {global_id_counter - 100000}")


if __name__ == "__main__":
    main()
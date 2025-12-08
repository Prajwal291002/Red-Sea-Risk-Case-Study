import sys
import pandas as pd
import numpy as np


def upsample_rates():
    """
    Read weekly rates data, upsample to hourly frequency using linear interpolation,
    add noise, and save the result to a CSV file.
    """
    print("🚀 Starting Rates-Only Upsampling...")

    # 1. READ WEEKLY RATES
    # We assume rates.csv exists and contains the real weekly data
    try:
        df_rates = pd.read_csv("rates.csv")
    except FileNotFoundError:
        print("❌ Error: 'rates.csv' not found. Please create it first.")
        sys.exit(1)

    # 2. SETUP DATE INDEX
    df_rates['Date'] = pd.to_datetime(df_rates['Date'])
    df_rates.set_index('Date', inplace=True)

    # 3. UPSAMPLE TO HOURLY (The "Big Data" Generator)
    # We turn ~17 rows into ~2,800 rows using linear interpolation
    df_hourly = df_rates.resample('h').interpolate(method='linear')

    # 4. CLEANUP & NOISE
    df_hourly.reset_index(inplace=True)
    df_hourly['Route'] = "Shanghai-Rotterdam"

    # Add "Micro-Noise" so the chart isn't a perfectly straight line
    # +/- $5 standard deviation
    noise = np.random.normal(0, 5, len(df_hourly))
    df_hourly['Price'] = df_hourly['Price'] + noise
    df_hourly['Price'] = df_hourly['Price'].round(2)

    # 5. SAVE
    output_file = "upsampled_rates.csv"
    df_hourly.to_csv(output_file, index=False)

    print(
        f"✅ SUCCESS: Expanded {len(df_rates)} weekly rows into "
        f"{len(df_hourly)} hourly rows."
    )
    print(f"📁 Saved to: {output_file}")


if __name__ == "__main__":
    upsample_rates()
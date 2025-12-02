# ═══════════════════════════════════════════════════════════
# Round 2 - Task 375
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 2686 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import numpy as np

# Helper functions for robust data processing
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        return float(v)
    return float(value)

def safe_get(df, column, default=None):
    """Safely get column from DataFrame, return default if not exists."""
    if isinstance(df, dict):
        return df.get(column, default)
    elif hasattr(df, 'columns') and column in df.columns:
        return df[column]
    return default

def execute_step():
    # Define the file path
    file_path = '/output/chunk6/data/context/payments.csv'
    
    # Load the payments.csv file into a dataframe
    try:
        df = pd.read_csv(file_path)
        print(f"Successfully loaded payments.csv with {len(df)} rows.")
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return

    # Filter for Merchant 'Belles_cookbook_store' and Card Scheme 'SwiftCharge'
    # Filter for March and April 2023 (Day of year 60 to 120)
    # 2023 is not a leap year.
    # Jan: 31, Feb: 28. Cumulative: 59.
    # March 1st is day 60.
    # March: 31 days. Cumulative: 90.
    # April: 30 days. Cumulative: 120.
    # Range: 60 <= day_of_year <= 120
    
    filtered_df = df[
        (df['merchant'] == 'Belles_cookbook_store') & 
        (df['card_scheme'] == 'SwiftCharge') &
        (df['day_of_year'] >= 60) &
        (df['day_of_year'] <= 120)
    ]

    print(f"Rows matching criteria (Merchant=Belles, Scheme=SwiftCharge, Mar-Apr): {len(filtered_df)}")

    if len(filtered_df) == 0:
        print("No transactions found matching the criteria.")
        return

    # Group by issuing_country and calculate average eur_amount
    result = filtered_df.groupby('issuing_country')['eur_amount'].mean()

    # Output the result in a readable format
    print("\nAverage transaction value grouped by issuing_country:")
    # Sort for consistent output
    for country, avg_amount in result.sort_index().items():
        print(f"{country}: {avg_amount:.2f}")

if __name__ == "__main__":
    execute_step()

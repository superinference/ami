# ═══════════════════════════════════════════════════════════
# Round 2 - Task 623
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3177 characters (FULL CODE)
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

def is_not_empty(array):
    """Check if array/list is not empty. Handles numpy arrays safely."""
    if array is None:
        return False
    if hasattr(array, 'size'):  # numpy array
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

def safe_index(array, idx, default=None):
    """Safely get array element at index, return default if out of bounds."""
    try:
        if 0 <= idx < len(array):
            return array[idx]
        return default
    except (IndexError, TypeError, AttributeError):
        return default

# Main Analysis Script
def main():
    # Load the payments dataset
    file_path = '/output/chunk3/data/context/payments.csv'
    print(f"Loading data from {file_path}...")
    df = pd.read_csv(file_path)

    # Define filter criteria
    target_merchant = 'Belles_cookbook_store'
    target_scheme = 'TransactPlus'
    # January and February 2023 correspond to days 1 through 59 (2023 is not a leap year)
    start_day = 1
    end_day = 59

    print(f"Filtering for Merchant: {target_merchant}, Scheme: {target_scheme}, Days: {start_day}-{end_day}")

    # Filter the DataFrame
    filtered_df = df[
        (df['merchant'] == target_merchant) & 
        (df['card_scheme'] == target_scheme) & 
        (df['day_of_year'] >= start_day) & 
        (df['day_of_year'] <= end_day)
    ]

    if filtered_df.empty:
        print("No transactions found matching the criteria.")
        return

    print(f"Found {len(filtered_df)} matching transactions.")

    # Calculate average transaction value grouped by device_type
    # We use 'eur_amount' for the transaction value
    avg_tx_value_by_device = filtered_df.groupby('device_type')['eur_amount'].mean()

    print("\nAverage Transaction Value by Device Type:")
    print(avg_tx_value_by_device)

    # Optional: Print in a more readable format for the final answer
    print("\nFormatted Result:")
    for device, avg_val in avg_tx_value_by_device.items():
        print(f"{device}: {avg_val:.2f}")

if __name__ == "__main__":
    main()

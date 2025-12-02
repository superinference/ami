# ═══════════════════════════════════════════════════════════
# Round 2 - Task 973
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3260 characters (FULL CODE)
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
def analyze_transactions():
    # Load the payments dataset
    file_path = '/output/chunk2/data/context/payments.csv'
    try:
        payments_df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return

    # Filter the data based on the specified criteria:
    # 1. Merchant is 'Belles_cookbook_store'
    # 2. Card scheme is 'SwiftCharge'
    # 3. Day of year is between 1 and 59 (January 1st to February 28th, 2023)
    # Note: 2023 is not a leap year, so Jan (31) + Feb (28) = 59 days.
    
    filtered_df = payments_df[
        (payments_df['merchant'] == 'Belles_cookbook_store') &
        (payments_df['card_scheme'] == 'SwiftCharge') &
        (payments_df['day_of_year'] >= 1) &
        (payments_df['day_of_year'] <= 59)
    ]

    # Check if data exists after filtering
    if filtered_df.empty:
        print("No transactions found matching the criteria.")
        return

    # Group by 'aci' and calculate the mean of 'eur_amount'
    # We use .groupby() on the 'aci' column and select 'eur_amount' for aggregation
    aci_groups = filtered_df.groupby('aci')['eur_amount'].mean()

    # Round the results to two decimal places as requested
    aci_groups_rounded = aci_groups.round(2)

    # Print the results
    print("Average transaction value grouped by ACI for Belles_cookbook_store (SwiftCharge, Jan-Feb 2023):")
    for aci, avg_value in aci_groups_rounded.items():
        print(f"{aci}: {avg_value}")

if __name__ == "__main__":
    analyze_transactions()

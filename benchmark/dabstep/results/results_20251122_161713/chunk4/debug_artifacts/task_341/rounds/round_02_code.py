# ═══════════════════════════════════════════════════════════
# Round 2 - Task 341
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3068 characters (FULL CODE)
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

# Define file path
payments_path = '/output/chunk4/data/context/payments.csv'

# Load the payments data
print("Loading payments data...")
df = pd.read_csv(payments_path)

# Apply filters as specified in the plan
# Merchant: Crossfit_Hanna
# Card Scheme: NexPay
# Day of Year: 244 to 304 (September 1st to October 31st)
print("Applying filters...")
filtered_df = df[
    (df['merchant'] == 'Crossfit_Hanna') &
    (df['card_scheme'] == 'NexPay') &
    (df['day_of_year'] >= 244) &
    (df['day_of_year'] <= 304)
]

# Display information about the filtered dataframe to verify the step
print(f"Original row count: {len(df)}")
print(f"Filtered row count: {len(filtered_df)}")

if not filtered_df.empty:
    # Group by 'issuing_country' and calculate the mean of 'eur_amount'
    print("\nCalculating average transaction value by issuing country...")
    average_tx_value = filtered_df.groupby('issuing_country')['eur_amount'].mean()
    
    # Sort for better readability (optional but helpful)
    average_tx_value = average_tx_value.sort_index()
    
    print("\nAverage Transaction Value by Issuing Country:")
    print(average_tx_value)
    
    # Also print formatted version for clarity
    print("\nFormatted Results:")
    for country, value in average_tx_value.items():
        print(f"{country}: {value:.2f}")
else:
    print("No transactions found matching the criteria.")

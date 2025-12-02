# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1784
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3089 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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


import pandas as pd

# Define file path
payments_path = '/output/chunk4/data/context/payments.csv'

# Load the payments data
df_payments = pd.read_csv(payments_path)

# Define filter criteria
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023

# Calculate day_of_year range for October 2023
# 2023 is a non-leap year.
# Days in months: Jan(31), Feb(28), Mar(31), Apr(30), May(31), Jun(30), Jul(31), Aug(31), Sep(30)
# Sum of days before October = 273
oct_start_day = 274  # October 1st
oct_end_day = 304    # October 31st

# Filter the DataFrame
# Criteria: Merchant matches, Year is 2023, Day of year is between 274 and 304 (inclusive)
filtered_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= oct_start_day) &
    (df_payments['day_of_year'] <= oct_end_day)
]

# Output the results
print(f"Filtering for merchant '{target_merchant}' in October {target_year} (Days {oct_start_day}-{oct_end_day})")
print(f"Number of transactions found: {len(filtered_txs)}")
print("\nFirst 5 transactions:")
print(filtered_txs.head())

# Show unique values in columns relevant for fee identification to verify data diversity
print("\nUnique values in key columns for fee matching:")
print(f"Card Schemes: {filtered_txs['card_scheme'].unique()}")
print(f"ACI: {filtered_txs['aci'].unique()}")
print(f"Is Credit: {filtered_txs['is_credit'].unique()}")

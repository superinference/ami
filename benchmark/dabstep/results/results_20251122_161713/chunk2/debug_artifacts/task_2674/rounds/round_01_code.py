# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2674
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2723 characters (FULL CODE)
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

# Load the payments dataset
file_path = '/output/chunk2/data/context/payments.csv'
payments = pd.read_csv(file_path)

# Define the filter criteria
target_merchant = 'Martinis_Fine_Steakhouse'
october_start = 274
october_end = 304

# Apply the filters
# 1. Filter for the specific merchant
merchant_mask = payments['merchant'] == target_merchant

# 2. Filter for October (day_of_year between 274 and 304 inclusive)
date_mask = (payments['day_of_year'] >= october_start) & (payments['day_of_year'] <= october_end)

# Combine masks and create the filtered DataFrame
filtered_transactions = payments[merchant_mask & date_mask].copy()

# Calculate verification metrics (to match Ground Truth)
total_volume = filtered_transactions['eur_amount'].sum()
transaction_count = len(filtered_transactions)

# Output the results
print(f"Successfully filtered transactions for {target_merchant} in October.")
print(f"Row count: {transaction_count}")
print(f"Total Volume: {total_volume:.2f}")
print("\nFirst 5 rows of filtered data:")
print(filtered_transactions.head())

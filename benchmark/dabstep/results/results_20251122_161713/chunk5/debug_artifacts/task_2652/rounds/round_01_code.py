# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2652
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2754 characters (FULL CODE)
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

# Define file paths
payments_path = '/output/chunk5/data/context/payments.csv'
output_path = 'august_transactions.csv'

# Load the payments data
print("Loading payments data...")
df = pd.read_csv(payments_path)

# Define filter criteria
target_merchant = 'Golfclub_Baron_Friso'
start_day = 213
end_day = 243

# Apply filters
# 1. Filter by Merchant
merchant_mask = df['merchant'] == target_merchant
# 2. Filter by Date (August: Day 213 to 243)
date_mask = (df['day_of_year'] >= start_day) & (df['day_of_year'] <= end_day)

# Combine masks
filtered_df = df[merchant_mask & date_mask].copy()

# Save the filtered data for the next step
filtered_df.to_csv(output_path, index=False)

# Print verification results
print(f"Total transactions in file: {len(df)}")
print(f"Transactions for {target_merchant}: {merchant_mask.sum()}")
print(f"Transactions in August (Day {start_day}-{end_day}): {date_mask.sum()}")
print(f"Final filtered count (Merchant + August): {len(filtered_df)}")
print(f"Filtered data saved to: {output_path}")
print("\nSample of filtered data:")
print(filtered_df.head())

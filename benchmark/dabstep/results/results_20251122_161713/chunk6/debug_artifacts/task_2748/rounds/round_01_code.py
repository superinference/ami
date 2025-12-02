# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2748
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2551 characters (FULL CODE)
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

# Load the payments data
payments_path = '/output/chunk6/data/context/payments.csv'
df = pd.read_csv(payments_path)

# Define the filter criteria
merchant_name = 'Crossfit_Hanna'
start_day = 305
end_day = 334

# Apply filters
# 1. Merchant is 'Crossfit_Hanna'
# 2. Transaction has a fraudulent dispute
# 3. Day of year is in November (305 to 334)
filtered_df = df[
    (df['merchant'] == merchant_name) &
    (df['has_fraudulent_dispute'] == True) &
    (df['day_of_year'] >= start_day) &
    (df['day_of_year'] <= end_day)
]

# Output the result for verification
print(f"Total rows in dataset: {len(df)}")
print(f"Rows after filtering for {merchant_name}, fraud=True, and Nov (days {start_day}-{end_day}): {len(filtered_df)}")
print("\nSample of filtered data:")
print(filtered_df[['psp_reference', 'merchant', 'day_of_year', 'has_fraudulent_dispute', 'aci', 'eur_amount']].head())

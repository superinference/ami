# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1837
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2478 characters (FULL CODE)
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

# Load the dataset
df = pd.read_csv(payments_path)

# Filter for 'Golfclub_Baron_Friso'
merchant_filter = df['merchant'] == 'Golfclub_Baron_Friso'

# Filter for March 2023 (Day of year 60 to 90 inclusive)
# 2023 is a non-leap year. Jan=31, Feb=28. 31+28=59. March starts on day 60. March has 31 days. 59+31=90.
date_filter = (df['day_of_year'] >= 60) & (df['day_of_year'] <= 90)

# Apply filters
filtered_df = df[merchant_filter & date_filter]

# Output the result summary
print(f"Loaded payments.csv with {len(df)} rows.")
print(f"Filtered for merchant 'Golfclub_Baron_Friso' in March 2023 (DOY 60-90).")
print(f"Number of transactions found: {len(filtered_df)}")
print("First 5 rows of filtered data:")
print(filtered_df.head())

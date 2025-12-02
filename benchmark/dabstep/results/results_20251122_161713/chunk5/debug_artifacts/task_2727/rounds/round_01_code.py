# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2727
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2837 characters (FULL CODE)
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
import datetime

# Load the payments dataset
payments_path = '/output/chunk5/data/context/payments.csv'
df = pd.read_csv(payments_path)

# Determine the day_of_year range for July 2023
# 2023 is not a leap year.
# Jan(31) + Feb(28) + Mar(31) + Apr(30) + May(31) + Jun(30) = 181 days
# July 1st is day 182
# July 31st is day 212
july_start = 182
july_end = 212

# Filter the dataset based on the specified criteria:
# 1. Merchant is 'Belles_cookbook_store'
# 2. Transaction has a fraudulent dispute (has_fraudulent_dispute == True)
# 3. Transaction date is in July (day_of_year between 182 and 212 inclusive)
filtered_df = df[
    (df['merchant'] == 'Belles_cookbook_store') &
    (df['has_fraudulent_dispute'] == True) &
    (df['day_of_year'] >= july_start) &
    (df['day_of_year'] <= july_end)
]

# Output the results to verify the filter
print(f"Filtered DataFrame Shape: {filtered_df.shape}")
print("First 5 rows of filtered data:")
print(filtered_df.head())

# Optional: Verify the date range in the filtered data
if not filtered_df.empty:
    print(f"\nDay of year range in filtered data: {filtered_df['day_of_year'].min()} - {filtered_df['day_of_year'].max()}")

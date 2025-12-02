# ═══════════════════════════════════════════════════════════
# Round 1 - Task 623
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2677 characters (FULL CODE)
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
file_path = '/output/chunk3/data/context/payments.csv'
df = pd.read_csv(file_path)

# Define the filter criteria
target_merchant = 'Belles_cookbook_store'
target_scheme = 'TransactPlus'
# January and February 2023 correspond to days 1 through 59 (2023 is not a leap year)
start_day = 1
end_day = 59

# Filter the DataFrame
# Conditions:
# 1. merchant is 'Belles_cookbook_store'
# 2. card_scheme is 'TransactPlus'
# 3. day_of_year is between 1 and 59 (inclusive)
filtered_df = df[
    (df['merchant'] == target_merchant) & 
    (df['card_scheme'] == target_scheme) & 
    (df['day_of_year'] >= start_day) & 
    (df['day_of_year'] <= end_day)
]

# Display the results of the filtering step
print(f"Total rows loaded: {len(df)}")
print(f"Rows after filtering for {target_merchant}, {target_scheme}, days {start_day}-{end_day}: {len(filtered_df)}")
print("\nSample of filtered data:")
print(filtered_df[['merchant', 'card_scheme', 'day_of_year', 'device_type', 'eur_amount']].head())

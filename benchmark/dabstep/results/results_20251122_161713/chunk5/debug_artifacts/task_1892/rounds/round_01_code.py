# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1892
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2399 characters (FULL CODE)
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
file_path = '/output/chunk5/data/context/payments.csv'
df = pd.read_csv(file_path)

# Filter the dataframe for the specific merchant 'Belles_cookbook_store'
# and the time period March 2023 (Day of Year 60 to 90)
# Note: 2023 is not a leap year. Jan (31) + Feb (28) = 59 days. 
# So March 1st is day 60, and March 31st is day 90.
filtered_df = df[
    (df['merchant'] == 'Belles_cookbook_store') & 
    (df['day_of_year'] >= 60) & 
    (df['day_of_year'] <= 90)
]

# Print the results to verify the filter
print(f"Number of transactions found: {len(filtered_df)}")
print(f"Total transaction volume: {filtered_df['eur_amount'].sum()}")
print("\nFirst 5 rows of filtered data:")
print(filtered_df.head())

# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1868
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2533 characters (FULL CODE)
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
payments_path = '/output/chunk5/data/context/payments.csv'
df = pd.read_csv(payments_path)

# Filter for merchant 'Rafa_AI'
merchant_name = 'Rafa_AI'
df_rafa = df[df['merchant'] == merchant_name]

# Filter for October 2023
# October 1st is day 274, October 31st is day 304 in a non-leap year (2023)
start_day = 274
end_day = 304
df_rafa_oct = df_rafa[(df_rafa['day_of_year'] >= start_day) & (df_rafa['day_of_year'] <= end_day)].copy()

# Verify the filtering
print(f"Filtered data for {merchant_name} in October (Days {start_day}-{end_day})")
print(f"Row count: {len(df_rafa_oct)}")
print("Sample rows:")
print(df_rafa_oct.head())

# Check unique values to ensure correctness
print("\nUnique days in filtered data:", sorted(df_rafa_oct['day_of_year'].unique()))
print("Unique merchants in filtered data:", df_rafa_oct['merchant'].unique())

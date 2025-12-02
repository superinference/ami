# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1853
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2591 characters (FULL CODE)
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
payments_path = '/output/chunk2/data/context/payments.csv'

# Load the payments dataframe
df_payments = pd.read_csv(payments_path)

# Filter for 'Martinis_Fine_Steakhouse' and July 2023 (Day of Year 182 to 212)
# 182 is July 1st, 212 is July 31st in a non-leap year (2023)
merchant_name = 'Martinis_Fine_Steakhouse'
start_day = 182
end_day = 212

filtered_df = df_payments[
    (df_payments['merchant'] == merchant_name) & 
    (df_payments['day_of_year'] >= start_day) & 
    (df_payments['day_of_year'] <= end_day)
].copy()

# Display the shape and first few rows to verify the filter
print(f"Filtered Data Shape: {filtered_df.shape}")
print(filtered_df.head())

# Optional: Verify the date range and merchant in the result
print(f"\nDate Range in Filtered Data: {filtered_df['day_of_year'].min()} to {filtered_df['day_of_year'].max()}")
print(f"Unique Merchants: {filtered_df['merchant'].unique()}")

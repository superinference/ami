# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2759
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2821 characters (FULL CODE)
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

# Set display options to ensure output is readable
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Define file path
payments_file = '/output/chunk4/data/context/payments.csv'

# Step 1: Load the payments.csv file
try:
    df_payments = pd.read_csv(payments_file)
    print(f"Successfully loaded {payments_file}. Total rows: {len(df_payments)}")
except FileNotFoundError:
    print(f"Error: File not found at {payments_file}")
    exit()

# Step 2: Filter for transactions where the merchant is 'Crossfit_Hanna' and the year is 2023
target_merchant = 'Crossfit_Hanna'
target_year = 2023

# Apply filter
filtered_df = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
]

# Output the results
print(f"Filtered data for merchant '{target_merchant}' in year {target_year}:")
print(f"Row count: {len(filtered_df)}")
print("\nFirst 5 rows of filtered data:")
print(filtered_df.head())

# Optional: Check unique card schemes in the filtered data (relevant for the overall goal)
print("\nUnique card schemes in filtered data:")
print(filtered_df['card_scheme'].unique())

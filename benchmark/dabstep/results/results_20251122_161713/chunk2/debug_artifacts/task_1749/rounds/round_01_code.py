# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1749
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2562 characters (FULL CODE)
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

# Load the payments data
print("Loading payments data...")
df_payments = pd.read_csv(payments_path)

# Filter for the specific merchant and year
merchant_name = 'Martinis_Fine_Steakhouse'
target_year = 2023

filtered_df = df_payments[
    (df_payments['merchant'] == merchant_name) & 
    (df_payments['year'] == target_year)
]

# Display the results of the filtering
print(f"Total rows in payments.csv: {len(df_payments)}")
print(f"Rows for {merchant_name} in {target_year}: {len(filtered_df)}")
print("\nSample of filtered data:")
print(filtered_df.head())

# Save the filtered dataframe to a pickle file for the next step to use efficiently
# This avoids reloading the huge CSV in subsequent steps
filtered_df.to_pickle('filtered_transactions.pkl')
print("\nFiltered data saved to 'filtered_transactions.pkl'")

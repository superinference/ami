# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2493
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2441 characters (FULL CODE)
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

# Define the file path for the payments dataset
payments_path = '/output/chunk4/data/context/payments.csv'

# Load the payments data
# Using pandas read_csv to load the data into a DataFrame
df_payments = pd.read_csv(payments_path)

# Filter the DataFrame based on the requirements:
# 1. Merchant is "Golfclub_Baron_Friso"
# 2. Year is 2023
filtered_df = df_payments[
    (df_payments['merchant'] == 'Golfclub_Baron_Friso') & 
    (df_payments['year'] == 2023)
]

# Print verification details
print(f"Successfully loaded payments.csv with {len(df_payments)} total rows.")
print(f"Filtered for merchant 'Golfclub_Baron_Friso' and year 2023.")
print(f"Resulting DataFrame has {len(filtered_df)} rows.")
print("\nFirst 5 rows of the filtered data:")
print(filtered_df.head())

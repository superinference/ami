# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1709
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2371 characters (FULL CODE)
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
payments_path = '/output/chunk3/data/context/payments.csv'

# Load the payments data
df_payments = pd.read_csv(payments_path)

# Filter for rows where merchant is "Rafa_AI", day_of_year is 300, and year is 2023
# Using boolean indexing for multiple conditions
filtered_transactions = df_payments[
    (df_payments['merchant'] == 'Rafa_AI') & 
    (df_payments['day_of_year'] == 300) & 
    (df_payments['year'] == 2023)
]

# Output the results to verify the filter
print(f"Total rows in payments.csv: {len(df_payments)}")
print(f"Transactions for Rafa_AI on day 300, 2023: {len(filtered_transactions)}")
print("\nSample of filtered transactions:")
print(filtered_transactions.head())

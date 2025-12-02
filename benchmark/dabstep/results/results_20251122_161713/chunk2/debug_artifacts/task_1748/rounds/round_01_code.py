# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1748
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2391 characters (FULL CODE)
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
payments_path = '/output/chunk2/data/context/payments.csv'

# Load the payments data
df_payments = pd.read_csv(payments_path)

# Filter for the specific merchant and year
# Merchant: 'Golfclub_Baron_Friso'
# Year: 2023
filtered_df = df_payments[
    (df_payments['merchant'] == 'Golfclub_Baron_Friso') & 
    (df_payments['year'] == 2023)
]

# Output results to verify the step
print(f"Total transactions found for Golfclub_Baron_Friso in 2023: {len(filtered_df)}")
print("First 5 rows of filtered data:")
print(filtered_df.head())

# Save the filtered dataframe to a variable or file if this were a persistent environment, 
# but for this single step execution, printing the verification is sufficient.

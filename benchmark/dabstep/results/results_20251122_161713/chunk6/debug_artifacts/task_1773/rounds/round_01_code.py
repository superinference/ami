# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1773
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2844 characters (FULL CODE)
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

# Load the payments.csv file
file_path = '/output/chunk6/data/context/payments.csv'
df = pd.read_csv(file_path)

# Filter for merchant 'Crossfit_Hanna'
merchant_filter = df['merchant'] == 'Crossfit_Hanna'

# Filter for November 2023
# November 1st is day 305, November 30th is day 334 in a non-leap year like 2023
date_filter = (df['year'] == 2023) & (df['day_of_year'] >= 305) & (df['day_of_year'] <= 334)

# Apply filters
filtered_df = df[merchant_filter & date_filter].copy()

# Display verification info
print(f"Original row count: {len(df)}")
print(f"Filtered row count (Crossfit_Hanna, Nov 2023): {len(filtered_df)}")

# Display first few rows to verify
print("\nFirst 5 rows of filtered data:")
print(filtered_df.head())

# Display unique values for columns relevant to fee rules (for upcoming steps)
print("\nUnique values in filtered data for fee matching:")
print(f"Card Schemes: {filtered_df['card_scheme'].unique()}")
print(f"Is Credit: {filtered_df['is_credit'].unique()}")
print(f"ACI: {filtered_df['aci'].unique()}")
print(f"Issuing Countries: {filtered_df['issuing_country'].unique()}")
print(f"Acquirer Countries: {filtered_df['acquirer_country'].unique()}")

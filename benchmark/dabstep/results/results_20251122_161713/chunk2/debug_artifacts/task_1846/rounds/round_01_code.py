# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1846
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2400 characters (FULL CODE)
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
df = pd.read_csv(payments_path)

# Filter for the specific merchant 'Golfclub_Baron_Friso'
merchant_name = 'Golfclub_Baron_Friso'
df_merchant = df[df['merchant'] == merchant_name]

# Filter for December 2023
# 2023 is a non-leap year. Days before Dec 1: 31+28+31+30+31+30+31+31+30+31+30 = 334.
# December starts on day 335.
df_filtered = df_merchant[df_merchant['day_of_year'] >= 335]

# Output verification stats
print(f"Total transactions for {merchant_name} in December 2023: {len(df_filtered)}")
print("\nSample transactions:")
print(df_filtered[['psp_reference', 'day_of_year', 'eur_amount', 'card_scheme', 'aci']].head())

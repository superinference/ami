# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2132
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2365 characters (FULL CODE)
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
payments_path = '/output/chunk3/data/context/payments.csv'
df = pd.read_csv(payments_path)

# Filter for transactions where the merchant is 'Golfclub_Baron_Friso'
# and the day_of_year is between 60 and 90 (inclusive) for March 2023
filtered_df = df[
    (df['merchant'] == 'Golfclub_Baron_Friso') & 
    (df['day_of_year'] >= 60) & 
    (df['day_of_year'] <= 90)
]

# Output the result to verify the step
print(f"Total rows loaded: {len(df)}")
print(f"Filtered rows for Golfclub_Baron_Friso in March (Day 60-90): {len(filtered_df)}")
print("\nSample of filtered data:")
print(filtered_df[['psp_reference', 'merchant', 'day_of_year', 'eur_amount', 'card_scheme']].head())

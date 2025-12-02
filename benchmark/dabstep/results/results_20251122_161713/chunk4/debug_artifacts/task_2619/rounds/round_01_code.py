# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2619
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2345 characters (FULL CODE)
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
payments_path = '/output/chunk4/data/context/payments.csv'

# Load the payments data
df = pd.read_csv(payments_path)

# Filter for merchant 'Crossfit_Hanna'
merchant_mask = df['merchant'] == 'Crossfit_Hanna'

# Filter for the month of May (Day of Year 121 to 151 inclusive)
date_mask = (df['day_of_year'] >= 121) & (df['day_of_year'] <= 151)

# Apply filters
filtered_df = df[merchant_mask & date_mask]

# Output verification stats to confirm against ground truth
print(f"Total transactions found: {len(filtered_df)}")
print(f"Average transaction amount: {filtered_df['eur_amount'].mean():.4f}")
print("\nSample of filtered data:")
print(filtered_df.head())

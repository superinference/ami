# ═══════════════════════════════════════════════════════════
# Round 1 - Task 625
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: sufficient
# Plan steps: 1
# Code length: 2399 characters (FULL CODE)
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
df = pd.read_csv('/output/chunk5/data/context/payments.csv')

# Filter the dataframe based on the specified criteria
# Merchant: 'Belles_cookbook_store'
# Card Scheme: 'TransactPlus'
# Year: 2023
# Day of Year: Between 60 and 120 (March and April)
filtered_df = df[
    (df['merchant'] == 'Belles_cookbook_store') &
    (df['card_scheme'] == 'TransactPlus') &
    (df['year'] == 2023) &
    (df['day_of_year'] >= 60) &
    (df['day_of_year'] <= 120)
]

# Calculate the average transaction value grouped by device_type
average_transaction_value = filtered_df.groupby('device_type')['eur_amount'].mean()

# Print the result
print("Average Transaction Value by Device Type:")
print(average_transaction_value)

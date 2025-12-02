# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2589
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2477 characters (FULL CODE)
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

# Load the payments dataset
file_path = '/output/chunk5/data/context/payments.csv'
df = pd.read_csv(file_path)

# Filter for transactions where the merchant is "Crossfit_Hanna"
# and the day_of_year is between 32 and 59 (February)
# 2023 is a non-leap year. Jan=31 days. Feb starts day 32, ends day 59.
feb_transactions = df[
    (df['merchant'] == 'Crossfit_Hanna') & 
    (df['day_of_year'] >= 32) & 
    (df['day_of_year'] <= 59)
]

# Output the results to verify the filter
print(f"Number of transactions found: {len(feb_transactions)}")
print("\nFirst 5 rows of filtered data:")
print(feb_transactions.head())

# Display unique card schemes in this subset (relevant for the steering question)
print("\nUnique Card Schemes in February for Crossfit_Hanna:")
print(feb_transactions['card_scheme'].unique())

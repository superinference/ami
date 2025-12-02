# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2419
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2676 characters (FULL CODE)
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
payments_path = '/output/chunk3/data/context/payments.csv'
df = pd.read_csv(payments_path)

# Filter for the specific merchant 'Rafa_AI'
# We create a copy to ensure we can modify the dataframe (add date column) without warnings
rafa_df = df[df['merchant'] == 'Rafa_AI'].copy()

# Convert 'year' and 'day_of_year' to a datetime object to easily filter by month
# The format '%Y%j' parses a year and day-of-year (e.g., 2023182)
rafa_df['date'] = pd.to_datetime(rafa_df['year'] * 1000 + rafa_df['day_of_year'], format='%Y%j')

# Filter for transactions that occurred in July 2023
# July is month 7
rafa_july_df = rafa_df[rafa_df['date'].dt.month == 7]

# Output the result to verify the step
print(f"Successfully loaded and filtered data.")
print(f"Merchant: Rafa_AI")
print(f"Time Period: July 2023")
print(f"Transaction Count: {len(rafa_july_df)}")
print("\nFirst 5 transactions:")
print(rafa_july_df[['date', 'merchant', 'card_scheme', 'is_credit', 'aci', 'eur_amount']].head())

# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1506
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2552 characters (FULL CODE)
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

# Load the fees.json file into a pandas DataFrame
fees_path = '/output/chunk6/data/context/fees.json'
df_fees = pd.read_json(fees_path)

# Inspect the dataframe structure
print("Fees DataFrame Info:")
print(df_fees.info())

# Inspect the specific columns requested: card_scheme, fixed_amount, and rate
print("\nFirst 10 rows of relevant columns:")
print(df_fees[['card_scheme', 'fixed_amount', 'rate']].head(10))

# To understand the "average scenario" mentioned in the goal, 
# let's look at the mean values for fixed_amount and rate per card scheme.
print("\nAverage Fee Structure by Card Scheme:")
print(df_fees.groupby('card_scheme')[['fixed_amount', 'rate']].mean())

# Also check the distribution of rates to see if there are outliers or specific tiers
print("\nDescriptive Statistics by Card Scheme:")
print(df_fees.groupby('card_scheme')[['fixed_amount', 'rate']].describe())

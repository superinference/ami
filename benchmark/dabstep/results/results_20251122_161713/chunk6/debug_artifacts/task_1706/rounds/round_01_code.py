# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1706
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2665 characters (FULL CODE)
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
payments_path = '/output/chunk6/data/context/payments.csv'
df_payments = pd.read_csv(payments_path)

# Filter for Rafa_AI, year 2023, day 12
filtered_txs = df_payments[
    (df_payments['merchant'] == 'Rafa_AI') & 
    (df_payments['year'] == 2023) & 
    (df_payments['day_of_year'] == 12)
]

# Display the results
print(f"Total transactions found: {len(filtered_txs)}")
print("\nSample of filtered transactions:")
print(filtered_txs.head())

# To help with the next step (Fee ID matching), let's look at the unique combinations 
# of attributes that drive fees (card_scheme, is_credit, aci, issuing_country, acquirer_country)
# This aligns with the 'Ground Truth' exploration mentioned in the prompt.
relevant_columns = ['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country']
unique_combinations = filtered_txs[relevant_columns].drop_duplicates()

print("\nUnique transaction attribute combinations for fee determination:")
print(unique_combinations)

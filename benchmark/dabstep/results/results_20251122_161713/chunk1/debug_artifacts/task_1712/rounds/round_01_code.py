# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1712
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2888 characters (FULL CODE)
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
payments_path = '/output/chunk1/data/context/payments.csv'

# Load the payments data
df_payments = pd.read_csv(payments_path)

# Filter the data based on the specified criteria
# Merchant: 'Belles_cookbook_store'
# Year: 2023
# Day of Year: 12
filtered_transactions = df_payments[
    (df_payments['merchant'] == 'Belles_cookbook_store') &
    (df_payments['year'] == 2023) &
    (df_payments['day_of_year'] == 12)
]

# Select relevant columns for the next step (fee calculation)
# We need amount, scheme, credit status, ACI, and country info to match fee rules later
columns_of_interest = [
    'psp_reference', 'merchant', 'card_scheme', 'eur_amount', 
    'is_credit', 'aci', 'ip_country', 'issuing_country', 'acquirer_country'
]

# Display the results
print(f"Number of transactions found: {len(filtered_transactions)}")
print("\nFiltered Transactions (First 5 rows):")
print(filtered_transactions[columns_of_interest].head().to_string())

# Print full list of amounts and schemes to verify against ground truth
print("\nFull Transaction Details for Verification:")
print(filtered_transactions[['eur_amount', 'card_scheme', 'aci', 'is_credit', 'issuing_country', 'acquirer_country']].to_string())

# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2708
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2592 characters (FULL CODE)
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
payments_path = '/output/chunk4/data/context/payments.csv'
df_payments = pd.read_csv(payments_path)

# Filter for transactions matching the criteria:
# 1. Merchant is 'Crossfit_Hanna'
# 2. Month is March (day_of_year between 60 and 90 inclusive)
# 3. Transaction has a fraudulent dispute
filtered_transactions = df_payments[
    (df_payments['merchant'] == 'Crossfit_Hanna') &
    (df_payments['day_of_year'] >= 60) &
    (df_payments['day_of_year'] <= 90) &
    (df_payments['has_fraudulent_dispute'] == True)
].copy()

# Display the filtered transactions to verify against ground truth
# Columns selected based on ground truth verification needs
columns_to_show = ['psp_reference', 'card_scheme', 'is_credit', 'eur_amount', 'issuing_country', 'acquirer_country', 'aci']
print(f"Filtered DataFrame shape: {filtered_transactions.shape}")
print(filtered_transactions[columns_to_show].to_string())

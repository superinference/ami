# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2549
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2659 characters (FULL CODE)
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
payments_path = '/output/chunk6/data/context/payments.csv'

# Load the payments data
df_payments = pd.read_csv(payments_path)

# Filter for merchant 'Rafa_AI'
# This is the specific step requested: filtering rows where merchant is 'Rafa_AI'
rafa_ai_transactions = df_payments[df_payments['merchant'] == 'Rafa_AI'].copy()

# Display the result to verify the step
print(f"Total rows in dataset: {len(df_payments)}")
print(f"Rows for Rafa_AI: {len(rafa_ai_transactions)}")
print("\nFirst 5 transactions for Rafa_AI:")
print(rafa_ai_transactions.head())

# Verify columns relevant for fee calculation exist
required_columns = ['eur_amount', 'card_scheme', 'aci', 'is_credit', 'issuing_country', 'acquirer_country']
missing_cols = [col for col in required_columns if col not in rafa_ai_transactions.columns]
if missing_cols:
    print(f"\nWarning: Missing columns for fee calculation: {missing_cols}")
else:
    print("\nAll required columns for fee calculation are present.")

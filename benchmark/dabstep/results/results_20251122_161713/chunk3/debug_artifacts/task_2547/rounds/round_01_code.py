# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2547
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2674 characters (FULL CODE)
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

# Define the file path
payments_path = '/output/chunk3/data/context/payments.csv'

# Load the payments dataset
df_payments = pd.read_csv(payments_path)

# Define the target merchant and year
target_merchant = "Martinis_Fine_Steakhouse"
target_year = 2023

# Filter the data for the specific merchant and year
# We check both conditions: merchant name match and year match
merchant_transactions = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
]

# Verify the filtering
print(f"Total transactions in file: {len(df_payments)}")
print(f"Transactions for '{target_merchant}' in {target_year}: {len(merchant_transactions)}")

# Display a sample of the filtered data to confirm structure
print("\nFirst 5 rows of filtered data:")
print(merchant_transactions.head())

# Display column names to ensure we have necessary fields for fee calculation (eur_amount, card_scheme, etc.)
print("\nColumns available:")
print(merchant_transactions.columns.tolist())

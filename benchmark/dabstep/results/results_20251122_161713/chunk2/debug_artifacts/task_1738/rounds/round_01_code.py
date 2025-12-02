# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1738
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3049 characters (FULL CODE)
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
import json

# Define file paths
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'
fees_path = '/output/chunk2/data/context/fees.json'

# 1. Load the payments data
df_payments = pd.read_csv(payments_path)

# 2. Filter the data
# Criteria: merchant is 'Rafa_AI', year is 2023, day_of_year is 200
filtered_txs = df_payments[
    (df_payments['merchant'] == 'Rafa_AI') & 
    (df_payments['year'] == 2023) & 
    (df_payments['day_of_year'] == 200)
].copy()

# 3. Load context data (needed for subsequent fee calculation steps)
# Load merchant data (contains account_type, merchant_category_code)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
df_merchant_data = pd.DataFrame(merchant_data)

# Load fee rules
with open(fees_path, 'r') as f:
    fees_rules = json.load(f)

# 4. Verification
print(f"Total transactions found for Rafa_AI on day 200: {len(filtered_txs)}")
print("\nSample transactions:")
print(filtered_txs[['merchant', 'card_scheme', 'eur_amount', 'issuing_country', 'acquirer_country', 'aci']].head())

# Check if count matches ground truth (84)
if len(filtered_txs) == 84:
    print("\nVerification Successful: Transaction count matches ground truth (84).")
else:
    print(f"\nWarning: Transaction count ({len(filtered_txs)}) does not match ground truth (84).")

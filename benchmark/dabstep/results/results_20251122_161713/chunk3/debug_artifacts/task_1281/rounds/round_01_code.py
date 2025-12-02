# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1281
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2897 characters (FULL CODE)
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
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

# Load payments data
df_payments = pd.read_csv(payments_path)

# Filter for transactions where card_scheme is 'GlobalCard' and is_credit is True
# We use .copy() to avoid SettingWithCopyWarning in subsequent steps
filtered_df = df_payments[
    (df_payments['card_scheme'] == 'GlobalCard') & 
    (df_payments['is_credit'] == True)
].copy()

# Load merchant data and fees (needed for subsequent analysis steps)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# Display verification statistics
print(f"Original dataset size: {len(df_payments)}")
print(f"Filtered dataset size (GlobalCard + Credit): {len(filtered_df)}")
print("\nFirst 5 rows of filtered data:")
print(filtered_df.head())

# Verify unique values in filtering columns to ensure correctness
print("\nUnique card_schemes in filtered data:", filtered_df['card_scheme'].unique())
print("Unique is_credit values in filtered data:", filtered_df['is_credit'].unique())

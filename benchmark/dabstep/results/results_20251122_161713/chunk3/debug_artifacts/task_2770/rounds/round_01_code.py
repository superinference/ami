# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2770
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2848 characters (FULL CODE)
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

# Load the payments dataset
df_payments = pd.read_csv(payments_path)

# Filter the data based on the specified criteria:
# 1. Merchant is "Martinis_Fine_Steakhouse"
# 2. Year is 2023
# 3. has_fraudulent_dispute is True
filtered_transactions = df_payments[
    (df_payments['merchant'] == "Martinis_Fine_Steakhouse") &
    (df_payments['year'] == 2023) &
    (df_payments['has_fraudulent_dispute'] == True)
]

# Display the results to verify the filter
print(f"Number of fraudulent transactions found: {len(filtered_transactions)}")
print("Sample of filtered transactions:")
print(filtered_transactions[['psp_reference', 'merchant', 'year', 'has_fraudulent_dispute', 'aci', 'eur_amount']].head())

# Optional: Load other context files to ensure they are accessible for next steps
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
    
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

print("\nContext files loaded successfully.")

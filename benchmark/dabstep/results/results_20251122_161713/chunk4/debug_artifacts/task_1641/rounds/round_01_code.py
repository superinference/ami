# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1641
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3400 characters (FULL CODE)
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
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_data_path = '/output/chunk4/data/context/merchant_data.json'

# Load the datasets
try:
    # Load payments.csv
    df_payments = pd.read_csv(payments_path)
    print(f"Successfully loaded payments.csv: {df_payments.shape}")

    # Load merchant_data.json
    df_merchants = pd.read_json(merchant_data_path)
    print(f"Successfully loaded merchant_data.json: {df_merchants.shape}")

    # Load fees.json
    df_fees = pd.read_json(fees_path)
    print(f"Successfully loaded fees.json: {df_fees.shape}")

    # PREPARATION STEP: Link Merchants to Transactions
    # We merge payments with merchant_data to attach 'account_type' and 'merchant_category_code' to each transaction
    df_merged = pd.merge(df_payments, df_merchants, on='merchant', how='left')
    
    print("\nMerged DataFrame Info:")
    print(f"Shape: {df_merged.shape}")
    print("Columns:", df_merged.columns.tolist())

    # Verification for the specific goal (Account Type F, GlobalCard)
    # Filter to see if we have relevant data
    target_subset = df_merged[
        (df_merged['account_type'] == 'F') & 
        (df_merged['card_scheme'] == 'GlobalCard')
    ]
    
    print(f"\nVerification: Found {len(target_subset)} transactions matching Account Type 'F' and Card Scheme 'GlobalCard'.")
    if not target_subset.empty:
        print("Sample of matching transactions:")
        print(target_subset[['merchant', 'card_scheme', 'account_type', 'aci', 'is_credit', 'eur_amount', 'issuing_country', 'acquirer_country']].head())

except Exception as e:
    print(f"Error loading or processing data: {e}")

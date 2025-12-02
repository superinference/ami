# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2506
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2554 characters (FULL CODE)
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
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'

# Load the datasets
try:
    df_payments = pd.read_csv(payments_path)
    df_fees = pd.read_json(fees_path)
    df_merchant_data = pd.read_json(merchant_data_path)
    
    print("Successfully loaded data files.")
    print(f"Payments shape: {df_payments.shape}")
    print(f"Fees shape: {df_fees.shape}")
    print(f"Merchant Data shape: {df_merchant_data.shape}")
    
    # Display first few rows to verify content
    print("\nPayments Sample:")
    print(df_payments.head(3))
    print("\nFees Sample:")
    print(df_fees.head(3))
    print("\nMerchant Data Sample:")
    print(df_merchant_data.head(3))

except Exception as e:
    print(f"Error loading files: {e}")

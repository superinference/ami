# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2499
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2924 characters (FULL CODE)
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
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

# Load payments.csv
try:
    df_payments = pd.read_csv(payments_path)
    print(f"Successfully loaded payments.csv with shape: {df_payments.shape}")
except Exception as e:
    print(f"Error loading payments.csv: {e}")

# Load fees.json
try:
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    df_fees = pd.DataFrame(fees_data)
    print(f"Successfully loaded fees.json with shape: {df_fees.shape}")
except Exception as e:
    print(f"Error loading fees.json: {e}")

# Load merchant_data.json
try:
    with open(merchant_data_path, 'r') as f:
        merchant_list = json.load(f)
    df_merchant_data = pd.DataFrame(merchant_list)
    print(f"Successfully loaded merchant_data.json with shape: {df_merchant_data.shape}")
except Exception as e:
    print(f"Error loading merchant_data.json: {e}")

# Display first few rows to verify content
print("\n--- Payments Head ---")
print(df_payments.head(2))
print("\n--- Fees Head ---")
print(df_fees.head(2))
print("\n--- Merchant Data Head ---")
print(df_merchant_data.head(2))

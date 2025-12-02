# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2757
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2778 characters (FULL CODE)
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
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'
fees_path = '/output/chunk6/data/context/fees.json'

# Load payments.csv
try:
    df_payments = pd.read_csv(payments_path)
    print(f"Successfully loaded payments.csv with shape: {df_payments.shape}")
except Exception as e:
    print(f"Error loading payments.csv: {e}")

# Load merchant_data.json
try:
    df_merchant_data = pd.read_json(merchant_data_path)
    print(f"Successfully loaded merchant_data.json with shape: {df_merchant_data.shape}")
except Exception as e:
    print(f"Error loading merchant_data.json: {e}")

# Load fees.json
try:
    df_fees = pd.read_json(fees_path)
    print(f"Successfully loaded fees.json with shape: {df_fees.shape}")
except Exception as e:
    print(f"Error loading fees.json: {e}")

# Display first few rows to verify content
print("\n--- Payments Head ---")
print(df_payments.head(2))
print("\n--- Merchant Data Head ---")
print(df_merchant_data.head(2))
print("\n--- Fees Head ---")
print(df_fees.head(2))

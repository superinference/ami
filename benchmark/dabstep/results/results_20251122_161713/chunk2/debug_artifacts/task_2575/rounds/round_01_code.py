# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2575
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2511 characters (FULL CODE)
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


import json
import pandas as pd

# Path to the fees file
fees_path = '/output/chunk2/data/context/fees.json'

# Load the fees data
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# Find the fee with ID 787
fee_787 = next((fee for fee in fees_data if fee['ID'] == 787), None)

if fee_787:
    print(f"Fee ID: {fee_787['ID']}")
    print(f"Current Criteria:")
    print(f"  - card_scheme: {fee_787.get('card_scheme')}")
    print(f"  - merchant_category_code: {fee_787.get('merchant_category_code')}")
    print(f"  - aci: {fee_787.get('aci')}")
    print(f"  - is_credit: {fee_787.get('is_credit')}")
    print(f"  - account_type: {fee_787.get('account_type')}")
    
    # Also printing the full object for context if needed later
    print("\nFull Fee Object:")
    print(json.dumps(fee_787, indent=2))
else:
    print("Fee with ID 787 not found.")

# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2524
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2631 characters (FULL CODE)
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

# Load fees.json
file_path = '/output/chunk2/data/context/fees.json'
with open(file_path, 'r') as f:
    fees = json.load(f)

# Find Fee ID 17
fee_17 = next((fee for fee in fees if fee['ID'] == 17), None)

if fee_17:
    print("Fee ID 17 Details:")
    print(json.dumps(fee_17, indent=4))
    
    # Extract specific criteria for matching
    criteria = {
        'card_scheme': fee_17.get('card_scheme'),
        'merchant_category_code': fee_17.get('merchant_category_code'),
        'aci': fee_17.get('aci'),
        'is_credit': fee_17.get('is_credit'),
        'account_type': fee_17.get('account_type'),
        'capture_delay': fee_17.get('capture_delay'),
        'monthly_fraud_level': fee_17.get('monthly_fraud_level'),
        'monthly_volume': fee_17.get('monthly_volume'),
        'intracountry': fee_17.get('intracountry')
    }
    print("\nExtracted Criteria for Matching:")
    print(criteria)
else:
    print("Fee ID 17 not found.")

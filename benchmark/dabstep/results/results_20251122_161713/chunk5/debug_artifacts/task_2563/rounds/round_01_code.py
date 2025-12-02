# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2563
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2834 characters (FULL CODE)
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

# Define file paths
fees_path = '/output/chunk5/data/context/fees.json'

# Load fees.json
try:
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    # Filter for Fee ID 384
    fee_384 = next((fee for fee in fees_data if fee['ID'] == 384), None)
    
    if fee_384:
        print(f"--- Fee ID 384 Characteristics ---")
        print(f"Card Scheme: {fee_384.get('card_scheme')}")
        print(f"Merchant Category Codes: {fee_384.get('merchant_category_code')}")
        print(f"Current Account Type: {fee_384.get('account_type')}")
        print(f"Full Entry: {json.dumps(fee_384, indent=2)}")
        
        # Check if account_type is a wildcard (empty list)
        if not fee_384.get('account_type'):
            print("\nObservation: 'account_type' is currently empty (Wildcard), meaning it applies to ALL account types matching the other criteria.")
        else:
            print(f"\nObservation: 'account_type' is currently restricted to: {fee_384.get('account_type')}")
            
    else:
        print("Fee with ID 384 not found.")

except Exception as e:
    print(f"Error loading or processing fees.json: {e}")

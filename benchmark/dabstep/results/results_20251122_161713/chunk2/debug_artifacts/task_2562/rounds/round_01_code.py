# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2562
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 2767 characters (FULL CODE)
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

# Define file path
fees_file_path = '/output/chunk2/data/context/fees.json'

# Load the fees data
try:
    with open(fees_file_path, 'r') as f:
        fees_data = json.load(f)
    
    # Filter for the row with ID 384
    fee_384 = next((fee for fee in fees_data if fee['ID'] == 384), None)
    
    if fee_384:
        print("Successfully loaded Fee ID 384.")
        print("Current Matching Criteria:")
        print(json.dumps(fee_384, indent=4))
        
        # Extract specific criteria relevant to the analysis
        print("\nExtracted Criteria for Analysis:")
        print(f"Current Account Type Settings: {fee_384.get('account_type')}")
        print(f"Card Scheme: {fee_384.get('card_scheme')}")
        print(f"Merchant Category Codes: {fee_384.get('merchant_category_code')}")
        print(f"ACI: {fee_384.get('aci')}")
        print(f"Is Credit: {fee_384.get('is_credit')}")
        print(f"Intracountry: {fee_384.get('intracountry')}")
    else:
        print("Fee with ID 384 not found in the dataset.")

except Exception as e:
    print(f"An error occurred: {e}")

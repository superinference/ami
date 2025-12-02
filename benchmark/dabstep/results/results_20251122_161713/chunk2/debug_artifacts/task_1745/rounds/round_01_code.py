# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1745
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3089 characters (FULL CODE)
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

# Define file path
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'

# Load merchant data
try:
    with open(merchant_data_path, 'r') as f:
        merchant_data = json.load(f)
    
    # Convert to DataFrame for easier handling
    df_merchant = pd.DataFrame(merchant_data)
    
    # Filter for the specific merchant "Rafa_AI"
    merchant_name = "Rafa_AI"
    rafa_attributes = df_merchant[df_merchant['merchant'] == merchant_name]
    
    if not rafa_attributes.empty:
        # Extract the first (and likely only) row as a dictionary
        attributes_dict = rafa_attributes.iloc[0].to_dict()
        
        # Extract specific fields requested in the plan
        mcc = attributes_dict.get('merchant_category_code')
        account_type = attributes_dict.get('account_type')
        capture_delay = attributes_dict.get('capture_delay')
        acquirers = attributes_dict.get('acquirer')
        
        print(f"Successfully retrieved attributes for {merchant_name}:")
        print(f"Merchant Category Code: {mcc}")
        print(f"Account Type: {account_type}")
        print(f"Capture Delay: {capture_delay}")
        print(f"Acquirers: {acquirers}")
        print("-" * 30)
        print("Full Attribute Dictionary:", attributes_dict)
    else:
        print(f"Merchant '{merchant_name}' not found in the data.")

except Exception as e:
    print(f"An error occurred: {e}")

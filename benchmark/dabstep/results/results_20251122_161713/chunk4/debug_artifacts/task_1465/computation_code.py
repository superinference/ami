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
fees_path = '/output/chunk4/data/context/fees.json'

# Load the fees.json file
try:
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    # Convert to DataFrame for easier inspection
    df_fees = pd.DataFrame(fees_data)
    
    # Inspect the dataframe structure
    print("--- DataFrame Info ---")
    print(df_fees.info())
    print("\n--- First 5 Rows ---")
    print(df_fees.head().to_string())
    
    # Inspect specific columns relevant to the question
    print("\n--- Sample of 'account_type' and 'aci' ---")
    print(df_fees[['ID', 'account_type', 'aci']].head(10).to_string())

    # Logic to identify Fee IDs for account_type = 'R' and aci = 'C'
    # Rules from context: 
    # - Fields are lists (arrays) or null.
    # - Null implies wildcard (applies to all).
    # - Empty list [] usually implies wildcard in these datasets (checking this assumption).
    
    target_account = 'R'
    target_aci = 'C'
    matching_ids = []

    for rule in fees_data:
        # Check Account Type
        # Match if None (Wildcard), Empty List (Wildcard), or 'R' is in the list
        acct_val = rule.get('account_type')
        acct_match = False
        if acct_val is None:
            acct_match = True
        elif isinstance(acct_val, list):
            if len(acct_val) == 0:
                acct_match = True
            elif target_account in acct_val:
                acct_match = True
        
        # Check ACI
        # Match if None (Wildcard), Empty List (Wildcard), or 'C' is in the list
        aci_val = rule.get('aci')
        aci_match = False
        if aci_val is None:
            aci_match = True
        elif isinstance(aci_val, list):
            if len(aci_val) == 0:
                aci_match = True
            elif target_aci in aci_val:
                aci_match = True
        
        # If both match, add ID
        if acct_match and aci_match:
            matching_ids.append(rule['ID'])

    print(f"\n--- Result: Fee IDs for Account Type '{target_account}' and ACI '{target_aci}' ---")
    print(matching_ids)

except Exception as e:
    print(f"Error: {e}")
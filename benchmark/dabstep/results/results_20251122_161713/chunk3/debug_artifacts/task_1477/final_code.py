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

def analyze_fees():
    # Load the fees.json file
    file_path = '/output/chunk3/data/context/fees.json'
    
    # Load json data
    with open(file_path, 'r') as f:
        fees_data = json.load(f)
    
    # Create DataFrame
    df_fees = pd.DataFrame(fees_data)
    
    # Inspect the dataframe structure
    print("Fees DataFrame Info:")
    print(df_fees.info())
    print("\nFirst 5 rows of fees data:")
    print(df_fees.head().to_string())
    
    # Filter for the specific question: account_type = H and aci = A
    # Logic: 
    # - account_type: Empty list [] or None is wildcard (applies to all). If list has values, 'H' must be in it.
    # - aci: Empty list [] or None is wildcard (applies to all). If list has values, 'A' must be in it.
    
    target_account = 'H'
    target_aci = 'A'
    
    matching_ids = []
    
    for index, row in df_fees.iterrows():
        # Check Account Type
        acc_type = row['account_type']
        acc_match = False
        if acc_type is None:
            acc_match = True
        elif isinstance(acc_type, list):
            if len(acc_type) == 0:
                acc_match = True
            elif target_account in acc_type:
                acc_match = True
        
        # Check ACI
        aci_val = row['aci']
        aci_match = False
        if aci_val is None:
            aci_match = True
        elif isinstance(aci_val, list):
            if len(aci_val) == 0:
                aci_match = True
            elif target_aci in aci_val:
                aci_match = True
                
        if acc_match and aci_match:
            matching_ids.append(row['ID'])
            
    print(f"\nFee IDs that apply to account_type='{target_account}' and aci='{target_aci}':")
    print(matching_ids)

if __name__ == "__main__":
    analyze_fees()
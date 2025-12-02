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

def analyze_expensive_mccs():
    # Load the fees.json file
    file_path = '/output/chunk5/data/context/fees.json'
    with open(file_path, 'r') as f:
        fees_data = json.load(f)
    
    # Create DataFrame
    df = pd.DataFrame(fees_data)
    
    # Inspect the relevant columns (fixed_amount, rate, merchant_category_code)
    # We calculate the fee for a 1 Euro transaction to find the most expensive rules.
    # Formula: fee = fixed_amount + (rate / 10000 * amount)
    amount = 1.0
    df['calculated_fee'] = df['fixed_amount'] + (df['rate'] / 10000 * amount)
    
    # Find the maximum fee value
    max_fee = df['calculated_fee'].max()
    
    # Filter the dataframe to get the rows with the maximum fee
    most_expensive_rules = df[df['calculated_fee'] == max_fee]
    
    # Extract the Merchant Category Codes (MCCs) from these rules
    expensive_mccs = []
    wildcard_present = False
    
    for index, row in most_expensive_rules.iterrows():
        mcc_list = row['merchant_category_code']
        
        # Check for wildcard (None or empty list means it applies to all)
        if mcc_list is None or (isinstance(mcc_list, list) and len(mcc_list) == 0):
            wildcard_present = True
        elif isinstance(mcc_list, list):
            expensive_mccs.extend(mcc_list)
        else:
            # Handle single value if it exists
            expensive_mccs.append(mcc_list)
            
    # Deduplicate and sort the list of MCCs
    unique_expensive_mccs = sorted(list(set(expensive_mccs)))
    
    # Output the results
    print(f"Maximum fee for 1 Euro transaction: {max_fee:.6f}")
    
    if wildcard_present:
        print("The most expensive rule applies to ALL MCCs (wildcard found).")
        if unique_expensive_mccs:
            print(f"Specific MCCs explicitly listed in max fee rules: {unique_expensive_mccs}")
    else:
        print(f"Most expensive MCCs: {unique_expensive_mccs}")

if __name__ == "__main__":
    analyze_expensive_mccs()
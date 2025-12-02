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
fees_path = '/output/chunk6/data/context/fees.json'
mcc_path = '/output/chunk6/data/context/merchant_category_codes.csv'

def find_most_expensive_mccs(transaction_amount):
    # Load fees data
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
        
    max_fee = -1.0
    
    # Step 1: Calculate fees for all rules and find the maximum fee
    for rule in fees_data:
        # Get fee components with defaults
        fixed = rule.get('fixed_amount')
        if fixed is None: fixed = 0.0
        
        rate = rule.get('rate')
        if rate is None: rate = 0
        
        # Calculate fee: fixed + (rate / 10000 * amount)
        fee = fixed + (rate / 10000.0 * transaction_amount)
        
        if fee > max_fee:
            max_fee = fee
            
    # Step 2: Identify MCCs associated with the maximum fee
    expensive_mccs = set()
    wildcard_max = False
    
    for rule in fees_data:
        fixed = rule.get('fixed_amount')
        if fixed is None: fixed = 0.0
        rate = rule.get('rate')
        if rate is None: rate = 0
        
        fee = fixed + (rate / 10000.0 * transaction_amount)
        
        # Check if this rule matches the max fee (using epsilon for float comparison)
        if abs(fee - max_fee) < 1e-9:
            mccs = rule.get('merchant_category_code')
            
            if mccs and len(mccs) > 0:
                # Add specific MCCs
                for mcc in mccs:
                    expensive_mccs.add(mcc)
            else:
                # Mark wildcard match (empty list or None means applies to all)
                wildcard_max = True
                
    # Step 3: Handle wildcard if necessary
    if wildcard_max:
        # If a wildcard rule is the most expensive, it applies to all MCCs
        # We load the full list of MCCs to provide a complete list
        try:
            df_mcc = pd.read_csv(mcc_path)
            all_mccs = df_mcc['mcc'].unique().tolist()
            expensive_mccs.update(all_mccs)
        except Exception as e:
            print(f"Error loading MCC list: {e}")
            
    return sorted(list(expensive_mccs))

# Execute for 50 euros
result = find_most_expensive_mccs(50.0)
print(result)
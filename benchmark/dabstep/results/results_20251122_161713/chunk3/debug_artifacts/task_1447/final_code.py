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

def calculate_max_fee_for_aci():
    # Load fees data
    file_path = '/output/chunk3/data/context/fees.json'
    with open(file_path, 'r') as f:
        fees_data = json.load(f)
    
    # Transaction parameters
    target_scheme = 'GlobalCard'
    target_is_credit = True
    amount = 10.0
    
    # Known ACIs from data context
    all_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    
    aci_max_fees = {}

    for aci in all_acis:
        matching_rules = []
        for rule in fees_data:
            # 1. Check Card Scheme
            if rule.get('card_scheme') != target_scheme:
                continue
            
            # 2. Check Credit Status
            # Rule applies if is_credit matches target (True) or is None (Wildcard)
            rule_credit = rule.get('is_credit')
            if rule_credit is not None and rule_credit != target_is_credit:
                continue
                
            # 3. Check ACI
            # Rule applies if aci list contains our ACI or is None (Wildcard)
            rule_aci = rule.get('aci')
            if rule_aci is not None and aci not in rule_aci:
                continue
            
            matching_rules.append(rule)
        
        # Calculate max fee for this ACI among all matching rules
        # If no rules match, we assume 0 or ignore, but typically there are base rules.
        current_max_fee = -1.0
        
        for rule in matching_rules:
            fixed = rule.get('fixed_amount', 0.0)
            rate = rule.get('rate', 0)
            
            # Formula: fee = fixed_amount + rate * transaction_value / 10000
            fee = fixed + (rate * amount / 10000.0)
            
            if fee > current_max_fee:
                current_max_fee = fee
        
        if current_max_fee != -1.0:
            aci_max_fees[aci] = current_max_fee
        else:
            aci_max_fees[aci] = 0.0

    # Sort to find the most expensive
    # Primary key: Fee (Descending)
    # Secondary key: ACI (Ascending) for alphabetical tie-break
    # We use a tuple (-fee, aci) to achieve this with standard sort
    sorted_acis = sorted(aci_max_fees.items(), key=lambda x: (-x[1], x[0]))
    
    if sorted_acis:
        winner_aci = sorted_acis[0][0]
        # Return as list as requested
        print([winner_aci])
    else:
        print([])

if __name__ == "__main__":
    calculate_max_fee_for_aci()
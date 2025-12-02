import pandas as pd
import json

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

def calculate_fee(amount, fee_rule):
    """Calculate fee based on amount and rule."""
    fixed = coerce_to_float(fee_rule.get('fixed_amount', 0))
    rate = coerce_to_float(fee_rule.get('rate', 0))
    # Fee formula: fixed + (rate * amount / 10000)
    return fixed + (rate * amount / 10000)

# Main analysis
def analyze_most_expensive_aci():
    # Load fees data
    file_path = '/output/chunk2/data/context/fees.json'
    with open(file_path, 'r') as f:
        fees_data = json.load(f)
    
    # Parameters from question
    target_scheme = 'NexPay'
    target_amount = 100.0
    target_is_credit = True
    
    # All possible ACIs
    all_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    
    # Dictionary to track max fee found for each ACI
    # Initialize with -1 to ensure we capture any positive fee
    max_fee_per_aci = {aci: -1.0 for aci in all_acis}
    
    # Iterate through all fee rules
    for rule in fees_data:
        # Check Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # Check is_credit
        # Rule applies if is_credit matches target OR is_credit is None (wildcard)
        rule_is_credit = rule.get('is_credit')
        if rule_is_credit is not None and rule_is_credit != target_is_credit:
            continue
            
        # Calculate fee for this rule
        fee = calculate_fee(target_amount, rule)
        
        # Determine applicable ACIs for this rule
        rule_acis = rule.get('aci')
        
        applicable_acis = []
        if is_not_empty(rule_acis):
            # Rule applies to specific ACIs
            applicable_acis = rule_acis
        else:
            # Rule applies to ALL ACIs (wildcard)
            applicable_acis = all_acis
            
        # Update max fee for applicable ACIs
        for aci in applicable_acis:
            if aci in max_fee_per_aci:
                if fee > max_fee_per_aci[aci]:
                    max_fee_per_aci[aci] = fee
    
    # Find the global maximum fee across all ACIs
    global_max_fee = -1.0
    for fee in max_fee_per_aci.values():
        if fee > global_max_fee:
            global_max_fee = fee
            
    # Identify ACIs that have this global max fee
    expensive_acis = []
    for aci, fee in max_fee_per_aci.items():
        # Use a small epsilon for float comparison if needed, but exact match is usually fine for calculated currency
        if abs(fee - global_max_fee) < 1e-9:
            expensive_acis.append(aci)
            
    # Sort alphabetically
    expensive_acis.sort()
    
    # Return the one with lowest alphabetical order (first in sorted list)
    # Question asks for response in a list
    if expensive_acis:
        result = [expensive_acis[0]]
    else:
        result = []
        
    print(result)

if __name__ == "__main__":
    analyze_most_expensive_aci()
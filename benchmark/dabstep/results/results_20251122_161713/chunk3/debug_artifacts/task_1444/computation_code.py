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

# Load the fees data
fees_path = '/output/chunk3/data/context/fees.json'
df_fees = pd.read_json(fees_path)

# Filter for 'NexPay' card scheme
df_nexpay = df_fees[df_fees['card_scheme'] == 'NexPay'].copy()

# Filter for credit transactions
# A rule applies to credit transactions if 'is_credit' is True or None (Wildcard)
# In pandas, None is typically represented as NaN
df_credit_rules = df_nexpay[
    (df_nexpay['is_credit'] == True) | 
    (df_nexpay['is_credit'].isna())
].copy()

# Define all unique ACIs based on the dataset description
all_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']

# Dictionary to store the maximum fee found for each ACI
aci_max_fees = {}
transaction_amount = 1.0

# Convert rules to a list of dictionaries for easier iteration over the 'aci' list field
rules = df_credit_rules.to_dict('records')

for aci in all_acis:
    max_fee = -1.0
    found_rule = False
    
    for rule in rules:
        rule_acis = rule.get('aci')
        
        # Determine if the rule applies to the current ACI
        # Rule applies if 'aci' field is None/NaN (Wildcard) or if the ACI is in the list
        is_applicable = False
        
        if rule_acis is None:
            is_applicable = True
        elif isinstance(rule_acis, float) and pd.isna(rule_acis):
            is_applicable = True
        elif isinstance(rule_acis, list):
            if aci in rule_acis:
                is_applicable = True
            elif len(rule_acis) == 0: # Treat empty list as wildcard (consistent with account_type logic)
                is_applicable = True
                
        if is_applicable:
            # Calculate fee: fixed_amount + (rate * amount / 10000)
            fixed = rule.get('fixed_amount')
            if pd.isna(fixed): fixed = 0.0
            
            rate = rule.get('rate')
            if pd.isna(rate): rate = 0.0
            
            fee = fixed + (rate * transaction_amount / 10000.0)
            
            if fee > max_fee:
                max_fee = fee
            found_rule = True
            
    if found_rule:
        aci_max_fees[aci] = max_fee
    else:
        # If no rule matches, we assume 0 or exclude. 
        # For "most expensive", 0 will naturally be at the bottom.
        aci_max_fees[aci] = 0.0

# Find the most expensive ACI
# Sort by Fee (Descending), then ACI (Ascending) for tie-breaking
sorted_acis = sorted(aci_max_fees.items(), key=lambda x: (-x[1], x[0]))

# Output the result
if sorted_acis:
    result = [sorted_acis[0][0]]
    print(result)
else:
    print([])
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

# Load the fees data
fees_file_path = '/output/chunk3/data/context/fees.json'
with open(fees_file_path, 'r') as f:
    fees_data = json.load(f)

# Parameters for the question
transaction_amount = 1.0
card_scheme = 'TransactPlus'
is_credit = True
# ACIs from manual/data (A-G)
all_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']

# Dictionary to track the maximum fee found for each ACI
# Initialize with 0.0
max_fee_by_aci = {aci: 0.0 for aci in all_acis}

# Iterate through all fee rules to find the most expensive scenario for each ACI
for rule in fees_data:
    # 1. Filter by Card Scheme
    if rule.get('card_scheme') != card_scheme:
        continue
        
    # 2. Filter by Credit status
    # Rule applies if is_credit matches OR if is_credit is None (Wildcard)
    rule_is_credit = rule.get('is_credit')
    if rule_is_credit is not None and rule_is_credit != is_credit:
        continue
        
    # 3. Calculate Fee for this rule
    # Formula: fee = fixed_amount + rate * transaction_value / 10000
    fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    
    calculated_fee = fixed_amount + (rate * transaction_amount / 10000.0)
    
    # 4. Apply fee to relevant ACIs
    rule_acis = rule.get('aci')
    
    # Determine which ACIs this rule applies to
    applicable_acis = []
    if rule_acis is None or len(rule_acis) == 0:
        # Wildcard: applies to ALL ACIs
        applicable_acis = all_acis
    else:
        # Specific list of ACIs
        applicable_acis = rule_acis
        
    # Update max fee for each applicable ACI
    for aci in applicable_acis:
        if aci in max_fee_by_aci: # Only consider valid ACIs
            if calculated_fee > max_fee_by_aci[aci]:
                max_fee_by_aci[aci] = calculated_fee

# Find the maximum fee across all ACIs
if not max_fee_by_aci:
    print("[]")
else:
    highest_fee = max(max_fee_by_aci.values())
    
    # Identify all ACIs that have this highest fee
    most_expensive_acis = [aci for aci, fee in max_fee_by_aci.items() if fee == highest_fee]
    
    # Sort alphabetically to handle ties (lowest alphabetical order)
    most_expensive_acis.sort()
    
    # Select the first one
    result = [most_expensive_acis[0]]
    
    # Print the result
    print(result)
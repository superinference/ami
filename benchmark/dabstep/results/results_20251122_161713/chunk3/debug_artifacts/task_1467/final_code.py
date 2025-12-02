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

import pandas as pd
import json

# Load the JSON file
file_path = '/output/chunk3/data/context/fees.json'

with open(file_path, 'r') as f:
    fees_data = json.load(f)

# Define target criteria from the question
target_account_type = 'R'
target_aci = 'E'

# Find matching IDs
# Logic: A fee rule applies if the specific field is a Wildcard (empty list or None) 
# OR if the target value is explicitly present in the list.
matching_ids = []

for rule in fees_data:
    # Check account_type
    rule_account_types = rule.get('account_type')
    # Match if wildcard (None/Empty) OR target is in list
    account_match = (not rule_account_types) or (target_account_type in rule_account_types)
    
    # Check aci
    rule_acis = rule.get('aci')
    # Match if wildcard (None/Empty) OR target is in list
    aci_match = (not rule_acis) or (target_aci in rule_acis)
    
    # If both conditions are compatible with the target transaction attributes
    if account_match and aci_match:
        matching_ids.append(rule['ID'])

# Sort and print result
matching_ids.sort()
if matching_ids:
    print(', '.join(map(str, matching_ids)))
else:
    print("No matching fee IDs found.")
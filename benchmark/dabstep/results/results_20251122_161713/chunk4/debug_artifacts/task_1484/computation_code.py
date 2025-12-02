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

# Load the JSON data
file_path = '/output/chunk4/data/context/fees.json'
with open(file_path, 'r') as f:
    fees_data = json.load(f)

# Filter logic
# Goal: Find fee IDs where account_type matches 'F' (or is wildcard) AND aci matches 'A' (or is wildcard)
# Wildcard rule: If a field is [] or None, it applies to ALL values.

matching_ids = []

for rule in fees_data:
    # Check account_type: Match if empty/None (wildcard) OR contains 'F'
    account_type_match = (not rule.get('account_type')) or ('F' in rule.get('account_type'))
    
    # Check aci: Match if empty/None (wildcard) OR contains 'A'
    aci_match = (not rule.get('aci')) or ('A' in rule.get('aci'))
    
    if account_type_match and aci_match:
        matching_ids.append(rule['ID'])

# Sort and format the output
matching_ids.sort()
result = ', '.join(map(str, matching_ids))

print(result)
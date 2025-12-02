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
file_path = '/output/chunk6/data/context/fees.json'
with open(file_path, 'r') as f:
    fees_data = json.load(f)

# Goal: Find fee IDs where account_type matches 'S' and aci matches 'D'
# Logic: 
#   - account_type match means: field is None OR field is empty list [] OR 'S' is in the list
#   - aci match means: field is None OR field is empty list [] OR 'D' is in the list

matching_ids = []

for rule in fees_data:
    # Check Account Type (S)
    # Wildcard if None or Empty List
    acct_types = rule.get('account_type')
    is_acct_match = (acct_types is None) or (not acct_types) or ('S' in acct_types)
    
    # Check ACI (D)
    # Wildcard if None or Empty List
    aci_types = rule.get('aci')
    is_aci_match = (aci_types is None) or (not aci_types) or ('D' in aci_types)
    
    if is_acct_match and is_aci_match:
        matching_ids.append(rule['ID'])

# Sort IDs numerically
matching_ids.sort()

# Format output as comma-separated string
result = ', '.join(map(str, matching_ids))

print(result)
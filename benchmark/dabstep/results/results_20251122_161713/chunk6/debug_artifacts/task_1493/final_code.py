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

def safe_index(array, idx, default=None):
    """Safely get array element at index, return default if out of bounds."""
    try:
        if 0 <= idx < len(array):
            return array[idx]
        return default
    except (IndexError, TypeError, AttributeError):
        return default

def solve():
    # Load the fees JSON file
    file_path = '/output/chunk6/data/context/fees.json'
    with open(file_path, 'r') as f:
        fees_data = json.load(f)

    # Define the target criteria
    target_account_type = 'S'
    target_aci = 'C'

    matching_ids = []

    # Iterate through each fee rule to check applicability
    for rule in fees_data:
        # Check account_type compatibility
        # Logic: Rule applies if account_type list is empty/None (Wildcard) OR if target is in the list
        rule_account_types = rule.get('account_type')
        is_account_match = False
        if not rule_account_types: # Empty list or None means it applies to ALL
            is_account_match = True
        elif target_account_type in rule_account_types:
            is_account_match = True

        # Check aci compatibility
        # Logic: Rule applies if aci list is empty/None (Wildcard) OR if target is in the list
        rule_acis = rule.get('aci')
        is_aci_match = False
        if not rule_acis: # Empty list or None means it applies to ALL
            is_aci_match = True
        elif target_aci in rule_acis:
            is_aci_match = True

        # If both conditions are met, add the ID
        if is_account_match and is_aci_match:
            matching_ids.append(rule['ID'])

    # Sort the IDs for clean output
    matching_ids.sort()

    # Output the result
    # The question asks for "fee ID or IDs", so we print them as a comma-separated list
    if matching_ids:
        print(", ".join(map(str, matching_ids)))
    else:
        print("No matching fee IDs found.")

if __name__ == "__main__":
    solve()
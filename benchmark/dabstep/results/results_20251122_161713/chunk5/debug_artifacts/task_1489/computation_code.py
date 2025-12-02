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

# Main execution
def main():
    # Load the fees data
    file_path = '/output/chunk5/data/context/fees.json'
    with open(file_path, 'r') as f:
        fees_data = json.load(f)

    # Define targets
    target_account_type = 'F'
    target_aci = 'F'

    matching_ids = []

    # Iterate through fee rules to find matches
    for rule in fees_data:
        # Check account_type
        # Logic: If the list is empty or None, it's a wildcard (matches everything).
        # If it's not empty, the target must be in the list.
        rule_account_type = rule.get('account_type')
        account_match = False
        if not is_not_empty(rule_account_type):
            account_match = True
        elif target_account_type in rule_account_type:
            account_match = True

        # Check aci
        # Logic: Same wildcard logic applies.
        rule_aci = rule.get('aci')
        aci_match = False
        if not is_not_empty(rule_aci):
            aci_match = True
        elif target_aci in rule_aci:
            aci_match = True

        # If both conditions match, add ID
        if account_match and aci_match:
            matching_ids.append(rule['ID'])

    # Sort results for consistent output
    matching_ids.sort()

    # Print result as comma-separated string
    if matching_ids:
        print(', '.join(map(str, matching_ids)))
    else:
        print("No matching fee IDs found.")

if __name__ == "__main__":
    main()
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
    # Define the file path
    file_path = '/output/chunk2/data/context/fees.json'

    # Load the JSON data
    try:
        with open(file_path, 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return

    # Target criteria
    target_account_type = 'D'
    target_aci = 'A'

    matching_ids = []

    # Iterate through each fee rule to check for matches
    for rule in fees_data:
        # Check account_type
        # Rule applies if account_type list is empty/None (wildcard) OR contains the target
        rule_account_types = rule.get('account_type')
        account_type_match = False
        if not rule_account_types: # Empty list or None implies wildcard (applies to all)
            account_type_match = True
        elif target_account_type in rule_account_types:
            account_type_match = True
            
        # Check aci
        # Rule applies if aci list is empty/None (wildcard) OR contains the target
        rule_aci = rule.get('aci')
        aci_match = False
        if not rule_aci: # Empty list or None implies wildcard (applies to all)
            aci_match = True
        elif target_aci in rule_aci:
            aci_match = True
            
        # If both conditions match, add ID to list
        if account_type_match and aci_match:
            matching_ids.append(rule.get('ID'))

    # Sort the IDs for consistent output
    matching_ids.sort()

    # Print the result
    # The question asks "What is the fee ID or IDs", so we print the list/values
    if matching_ids:
        print(", ".join(map(str, matching_ids)))
    else:
        print("No matching fee IDs found.")

if __name__ == "__main__":
    main()
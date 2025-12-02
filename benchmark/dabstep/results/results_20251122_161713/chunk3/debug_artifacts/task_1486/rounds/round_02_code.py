# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1486
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 2884 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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

def main():
    # Load the fees.json file
    # Using json.load is safer for nested list structures than pd.read_json
    file_path = '/output/chunk3/data/context/fees.json'
    with open(file_path, 'r') as f:
        fees_data = json.load(f)

    # Define the target criteria
    target_account_type = 'F'
    target_aci = 'C'

    matching_ids = []

    # Iterate through each fee rule to check for applicability
    for rule in fees_data:
        # Check account_type
        # A rule applies if the list is empty/None (wildcard) OR if the target is in the list
        rule_account_type = rule.get('account_type')
        if not rule_account_type:  # Handles None and empty list []
            account_match = True
        else:
            account_match = target_account_type in rule_account_type

        # Check aci
        # A rule applies if the list is empty/None (wildcard) OR if the target is in the list
        rule_aci = rule.get('aci')
        if not rule_aci:  # Handles None and empty list []
            aci_match = True
        else:
            aci_match = target_aci in rule_aci

        # If both conditions are met, the fee ID applies
        if account_match and aci_match:
            matching_ids.append(rule['ID'])

    # Output the result
    # Sort IDs for consistent output
    matching_ids.sort()
    
    if matching_ids:
        print(', '.join(map(str, matching_ids)))
    else:
        print("No matching fee IDs found.")

if __name__ == "__main__":
    main()

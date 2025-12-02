# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1473
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 2945 characters (FULL CODE)
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

def safe_index(array, idx, default=None):
    """Safely get array element at index, return default if out of bounds."""
    try:
        if 0 <= idx < len(array):
            return array[idx]
        return default
    except (IndexError, TypeError, AttributeError):
        return default

# Main analysis
def main():
    # Load fees data
    fees_file_path = '/output/chunk4/data/context/fees.json'
    with open(fees_file_path, 'r') as f:
        fees = json.load(f)

    # Target criteria
    target_account_type = 'D'
    target_aci = 'D'

    # Filter fees based on criteria
    # Logic: A fee applies if the field is a wildcard (None or empty list) OR if the target value is in the list
    matching_ids = []
    
    for rule in fees:
        # Check account_type
        # Rule applies if account_type is empty/null (wildcard) OR contains 'D'
        account_type_rule = rule.get('account_type')
        account_match = (not account_type_rule) or (target_account_type in account_type_rule)
        
        # Check aci
        # Rule applies if aci is empty/null (wildcard) OR contains 'D'
        aci_rule = rule.get('aci')
        aci_match = (not aci_rule) or (target_aci in aci_rule)
        
        if account_match and aci_match:
            matching_ids.append(rule['ID'])

    # Sort and format output
    matching_ids.sort()
    
    # Print result as comma-separated string
    if matching_ids:
        print(", ".join(map(str, matching_ids)))
    else:
        print("No matching fee IDs found.")

if __name__ == "__main__":
    main()

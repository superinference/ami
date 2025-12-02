# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1463
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3238 characters (FULL CODE)
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

# Main execution
def main():
    # Load fees.json
    fees_path = '/output/chunk6/data/context/fees.json'
    try:
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
    except Exception as e:
        print(f"Error loading fees.json: {e}")
        return

    # Target criteria
    target_account_type = 'R'
    target_aci = 'A'

    matching_ids = []

    for rule in fees_data:
        # Check Account Type
        # Logic: If list is empty/None, it's a wildcard (matches all). 
        # Otherwise, target must be in the list.
        rule_account_types = rule.get('account_type')
        if not is_not_empty(rule_account_types):
            account_match = True # Wildcard matches everything
        else:
            account_match = target_account_type in rule_account_types

        # Check ACI
        # Logic: If list is empty/None, it's a wildcard (matches all).
        # Otherwise, target must be in the list.
        rule_aci = rule.get('aci')
        if not is_not_empty(rule_aci):
            aci_match = True # Wildcard matches everything
        else:
            aci_match = target_aci in rule_aci

        # If both match, add ID
        if account_match and aci_match:
            matching_ids.append(rule.get('ID'))

    # Sort and print results
    matching_ids.sort()
    
    # Format output as comma-separated string if multiple, or single value
    if matching_ids:
        print(", ".join(map(str, matching_ids)))
    else:
        print("No matching fee IDs found.")

if __name__ == "__main__":
    main()

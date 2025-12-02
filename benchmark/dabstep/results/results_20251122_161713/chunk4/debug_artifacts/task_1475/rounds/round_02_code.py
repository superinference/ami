# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1475
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3565 characters (FULL CODE)
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
    file_path = '/output/chunk4/data/context/fees.json'
    
    try:
        # Load fees data
        with open(file_path, 'r') as f:
            fees_data = json.load(f)
            
        print(f"Successfully loaded {len(fees_data)} fee rules.")
        
        # Target criteria
        target_account_type = 'D'
        target_aci = 'F'
        
        matching_ids = []
        
        for rule in fees_data:
            # Check Account Type
            # Rule: If field is null or empty list, it applies to ALL values (Wildcard)
            rule_account_types = rule.get('account_type')
            account_match = False
            if not rule_account_types: # None or empty list -> Wildcard match
                account_match = True
            elif target_account_type in rule_account_types:
                account_match = True
                
            # Check ACI
            # Rule: If field is null or empty list, it applies to ALL values (Wildcard)
            rule_aci = rule.get('aci')
            aci_match = False
            if not rule_aci: # None or empty list -> Wildcard match
                aci_match = True
            elif target_aci in rule_aci:
                aci_match = True
            
            # If both match, add ID
            if account_match and aci_match:
                matching_ids.append(rule['ID'])
        
        # Sort results for consistent output
        matching_ids.sort()
        
        # Output result
        if matching_ids:
            print(f"Matching Fee IDs: {matching_ids}")
            # Print formatted for final answer extraction if needed
            print(", ".join(map(str, matching_ids)))
        else:
            print("No matching fee IDs found.")
            
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

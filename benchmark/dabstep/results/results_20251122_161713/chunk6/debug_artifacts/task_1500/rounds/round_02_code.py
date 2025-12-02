# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1500
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 2761 characters (FULL CODE)
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

# Main analysis script
def main():
    # File path
    fees_path = '/output/chunk6/data/context/fees.json'
    
    # Load fees data
    # Using json.load is often more robust for handling list fields in JSON than pandas read_json
    with open(fees_path, 'r') as f:
        fees = json.load(f)
    
    # Target criteria
    target_account_type = 'O'
    target_aci = 'C'
    
    # Filter fees based on criteria
    # Logic: A fee rule applies if the specific field is a Wildcard (empty/None) OR contains the target value
    matching_ids = []
    
    for rule in fees:
        # Check account_type
        # Rule applies if list is empty/None (Wildcard) OR 'O' is in the list
        rule_account_types = rule.get('account_type')
        account_match = (not rule_account_types) or (target_account_type in rule_account_types)
        
        # Check aci
        # Rule applies if list is empty/None (Wildcard) OR 'C' is in the list
        rule_acis = rule.get('aci')
        aci_match = (not rule_acis) or (target_aci in rule_acis)
        
        if account_match and aci_match:
            matching_ids.append(rule['ID'])
            
    # Output results
    # Sort IDs for consistent output
    matching_ids.sort()
    
    # Format as comma-separated string
    result = ', '.join(map(str, matching_ids))
    print(result)

if __name__ == "__main__":
    main()

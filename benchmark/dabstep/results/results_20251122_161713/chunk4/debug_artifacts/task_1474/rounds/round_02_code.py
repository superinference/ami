# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1474
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3226 characters (FULL CODE)
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

# Main Execution
def main():
    fees_file_path = '/output/chunk4/data/context/fees.json'
    
    try:
        # Load the JSON data directly to handle list fields and nulls robustly
        # Using json.load is preferred over pd.read_json for nested list logic and wildcard handling
        with open(fees_file_path, 'r') as f:
            fees = json.load(f)
            
        # Target criteria from the question
        target_account_type = 'D'
        target_aci = 'E'
        
        matching_ids = []
        
        # Iterate through rules to find matches
        # Logic: A rule applies if the field is a Wildcard (None or empty) OR if it explicitly contains the target value
        for rule in fees:
            # Check account_type
            # Wildcard handling: [] or None = matches ALL
            rule_account_types = rule.get('account_type')
            # If rule_account_types is None or empty list, it evaluates to False in boolean context (not rule_account_types is True)
            account_match = (not rule_account_types) or (target_account_type in rule_account_types)
            
            # Check aci
            # Wildcard handling: [] or None = matches ALL
            rule_acis = rule.get('aci')
            aci_match = (not rule_acis) or (target_aci in rule_acis)
            
            if account_match and aci_match:
                matching_ids.append(rule['ID'])
        
        # Sort results for consistent output
        matching_ids.sort()
        
        # Format result as comma-separated string
        if matching_ids:
            result = ', '.join(map(str, matching_ids))
        else:
            result = "No matching fees found"
            
        print(result)
        
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

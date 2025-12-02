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
        # Load the JSON file
        with open(file_path, 'r') as f:
            fees_data = json.load(f)
        
        # Define target criteria
        target_account_type = 'O'
        target_aci = 'G'
        
        matching_ids = []
        
        # Iterate through each fee rule to find matches
        for rule in fees_data:
            # Check account_type
            # Rule applies if account_type list is empty/null (wildcard) OR contains target
            rule_account_types = rule.get('account_type')
            account_match = False
            # Use helper to check if list is populated
            if not is_not_empty(rule_account_types): # Wildcard (None or []) matches ALL
                account_match = True
            elif target_account_type in rule_account_types: # Explicit match
                account_match = True
            
            # Check aci
            # Rule applies if aci list is empty/null (wildcard) OR contains target
            rule_acis = rule.get('aci')
            aci_match = False
            # Use helper to check if list is populated
            if not is_not_empty(rule_acis): # Wildcard (None or []) matches ALL
                aci_match = True
            elif target_aci in rule_acis: # Explicit match
                aci_match = True
                
            # If both conditions are satisfied, add ID to results
            if account_match and aci_match:
                matching_ids.append(rule['ID'])
        
        # Output result
        if not matching_ids:
            print("No matching fee IDs found.")
        else:
            # Sort for consistent output and print as comma-separated string
            matching_ids.sort()
            print(", ".join(map(str, matching_ids)))
            
    except Exception as e:
        print(f"Error processing: {e}")

if __name__ == "__main__":
    main()
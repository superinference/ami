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


import json

def solve():
    # Load the fees.json file
    file_path = '/output/chunk2/data/context/fees.json'
    with open(file_path, 'r') as f:
        fees_data = json.load(f)
    
    # Define the criteria from the question
    target_account_type = 'H'
    target_aci = 'D'
    
    matching_ids = []
    
    # Iterate through each fee rule to check for matches
    for rule in fees_data:
        # Check account_type
        # Logic: Matches if the rule's account_type is None (wildcard), empty list (wildcard), 
        # or contains the target 'H'.
        rule_account_type = rule.get('account_type')
        account_match = False
        if rule_account_type is None or len(rule_account_type) == 0:
            account_match = True
        elif target_account_type in rule_account_type:
            account_match = True
            
        # Check aci
        # Logic: Matches if the rule's aci is None (wildcard), empty list (wildcard), 
        # or contains the target 'D'.
        rule_aci = rule.get('aci')
        aci_match = False
        if rule_aci is None or len(rule_aci) == 0:
            aci_match = True
        elif target_aci in rule_aci:
            aci_match = True
            
        # If both criteria match, add the ID to the list
        if account_match and aci_match:
            matching_ids.append(rule['ID'])
            
    # Print the resulting ID(s)
    print(sorted(matching_ids))

if __name__ == "__main__":
    solve()
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

# Set file path based on context
FEES_FILE_PATH = '/output/chunk5/data/context/fees.json'

def get_most_expensive_aci():
    # Load fees data
    try:
        with open(FEES_FILE_PATH, 'r') as f:
            fees = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {FEES_FILE_PATH}")
        return []

    # Transaction details
    # Question: "For a credit transaction of 10 euros on NexPay"
    target_scheme = 'NexPay'
    target_is_credit = True
    target_amount = 10.0
    
    # Valid ACIs to consider (from data context)
    valid_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    
    # Dictionary to track the maximum fee found for each ACI
    # Initialize with 0.0
    max_fees = {aci: 0.0 for aci in valid_acis}
    
    for rule in fees:
        # 1. Check Card Scheme
        if rule.get('card_scheme') != target_scheme:
            continue
            
        # 2. Check Credit Status
        # Rule applies if 'is_credit' matches target OR is None (wildcard)
        rule_credit = rule.get('is_credit')
        if rule_credit is not None and rule_credit != target_is_credit:
            continue
            
        # 3. Calculate Fee
        # Formula: fee = fixed_amount + (rate * amount / 10000)
        # Use safe gets and float conversion
        fixed = float(rule.get('fixed_amount', 0.0) or 0.0)
        rate = float(rule.get('rate', 0.0) or 0.0)
        
        fee = fixed + (rate * target_amount / 10000.0)
        
        # 4. Update Max Fee for applicable ACIs
        rule_acis = rule.get('aci')
        
        if rule_acis is None:
            # Wildcard: Rule applies to ALL ACIs
            for aci in valid_acis:
                if fee > max_fees[aci]:
                    max_fees[aci] = fee
        else:
            # List: Rule applies to specific ACIs listed
            for aci in rule_acis:
                if aci in valid_acis:
                    if fee > max_fees[aci]:
                        max_fees[aci] = fee
                        
    # Find the highest fee value
    if not max_fees:
        return []
        
    highest_fee_value = max(max_fees.values())
    
    # Identify all ACIs that share this highest fee
    most_expensive_acis = [aci for aci, fee in max_fees.items() if fee == highest_fee_value]
    
    # Sort alphabetically to handle draws (lowest alphabetical order)
    most_expensive_acis.sort()
    
    # Return the top result as a list
    return [most_expensive_acis[0]] if most_expensive_acis else []

# Execute and print result
print(get_most_expensive_aci())
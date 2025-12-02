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


import pandas as pd

# Load the fees.json file into a dataframe
fees_path = '/output/chunk6/data/context/fees.json'
df_fees = pd.read_json(fees_path)

# Define the target criteria
target_account_type = 'F'
target_aci = 'B'

# Function to determine if a fee rule applies based on account_type and aci
def check_rule_applicability(row):
    # Check account_type: Empty list [] or None means it applies to ALL (Wildcard)
    # Otherwise, the specific type must be in the list
    account_type_rule = row['account_type']
    if account_type_rule is None or len(account_type_rule) == 0:
        account_match = True
    else:
        account_match = target_account_type in account_type_rule
        
    # Check aci: Empty list [] or None means it applies to ALL (Wildcard)
    # Otherwise, the specific aci must be in the list
    aci_rule = row['aci']
    if aci_rule is None or len(aci_rule) == 0:
        aci_match = True
    else:
        aci_match = target_aci in aci_rule
        
    return account_match and aci_match

# Filter the dataframe for matching rules
matching_fees = df_fees[df_fees.apply(check_rule_applicability, axis=1)]

# Extract the IDs
matching_ids = matching_fees['ID'].tolist()

# Print the result
print(matching_ids)
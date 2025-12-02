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

# Define the file path for fees.json
fees_file_path = '/output/chunk2/data/context/fees.json'

# Load the JSON data
with open(fees_file_path, 'r') as f:
    fees_data = json.load(f)

# Constants for the scenario
TRANSACTION_AMOUNT = 10.0
TARGET_SCHEME = 'TransactPlus'
IS_CREDIT_TRANSACTION = True
POSSIBLE_ACIS = ['A', 'B', 'C', 'D', 'E', 'F', 'G']

# Dictionary to track the maximum fee found for each ACI
# Initialize with -1.0 to ensure we capture any positive fee
aci_max_fees = {aci: -1.0 for aci in POSSIBLE_ACIS}

# Iterate through all fee rules to find the most expensive scenario for each ACI
for rule in fees_data:
    # 1. Check Card Scheme
    if rule.get('card_scheme') != TARGET_SCHEME:
        continue
        
    # 2. Check Credit/Debit applicability
    # The transaction is Credit.
    # Rule applies if is_credit is True (specific to credit) or None (applies to both).
    # Rule does NOT apply if is_credit is False (specific to debit).
    rule_is_credit = rule.get('is_credit')
    if rule_is_credit is False:
        continue
        
    # 3. Calculate Fee for this rule
    # Formula: fixed_amount + (rate * amount / 10000)
    fixed = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    
    calculated_fee = fixed + (rate * TRANSACTION_AMOUNT / 10000.0)
    
    # 4. Determine which ACIs this rule applies to
    rule_acis = rule.get('aci')
    
    applicable_acis_for_rule = []
    if rule_acis is None or len(rule_acis) == 0:
        # Wildcard: applies to ALL ACIs
        applicable_acis_for_rule = POSSIBLE_ACIS
    else:
        # Specific list of ACIs
        applicable_acis_for_rule = rule_acis
        
    # 5. Update max fee for applicable ACIs
    for aci in applicable_acis_for_rule:
        # Only track valid ACIs (A-G)
        if aci in aci_max_fees:
            if calculated_fee > aci_max_fees[aci]:
                aci_max_fees[aci] = calculated_fee

# Find the global maximum fee across all ACIs
if not aci_max_fees:
    print([])
else:
    max_fee_value = max(aci_max_fees.values())
    
    # Identify all ACIs that have this maximum fee
    most_expensive_acis = [aci for aci, fee in aci_max_fees.items() if fee == max_fee_value]
    
    # Sort alphabetically to handle ties (lowest alphabetical order)
    most_expensive_acis.sort()
    
    # Select the first one (lowest alphabetical order)
    # Return as a list as requested
    result = [most_expensive_acis[0]]
    
    print(result)
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

def analyze_expensive_aci():
    # Load the fees.json file
    file_path = '/output/chunk2/data/context/fees.json'
    with open(file_path, 'r') as f:
        fees_data = json.load(f)

    # Constants for the specific question
    TARGET_SCHEME = 'NexPay'
    TRANSACTION_AMOUNT = 10000.0
    
    # Defined ACIs from manual/data
    ALL_ACIS = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    
    # Dictionary to store the maximum fee found for each ACI
    # Initialize with 0.0
    max_fee_per_aci = {aci: 0.0 for aci in ALL_ACIS}
    
    # Iterate through all fee rules
    for rule in fees_data:
        # 1. Filter by Card Scheme
        if rule.get('card_scheme') != TARGET_SCHEME:
            continue
            
        # 2. Filter by Credit/Debit
        # The transaction is Credit.
        # Rule applies if rule['is_credit'] is True OR None (wildcard).
        # Rule does NOT apply if rule['is_credit'] is False (Debit only).
        rule_is_credit = rule.get('is_credit')
        if rule_is_credit is False:
            continue
            
        # 3. Calculate Fee for this rule
        # Formula: fee = fixed_amount + rate * transaction_value / 10000
        fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
        rate = coerce_to_float(rule.get('rate', 0))
        
        # Since amount is 10000, rate * 10000 / 10000 simplifies to rate
        calculated_fee = fixed_amount + (rate * TRANSACTION_AMOUNT / 10000.0)
        
        # 4. Determine which ACIs this rule applies to
        rule_acis = rule.get('aci')
        
        applicable_acis = []
        if rule_acis is None or len(rule_acis) == 0:
            # Wildcard: Applies to ALL ACIs
            applicable_acis = ALL_ACIS
        else:
            # Specific list
            applicable_acis = rule_acis
            
        # 5. Update max fee for the applicable ACIs
        # We are looking for the "most expensive" scenario for each ACI
        for aci in applicable_acis:
            # Only consider known ACIs (A-G)
            if aci in max_fee_per_aci:
                if calculated_fee > max_fee_per_aci[aci]:
                    max_fee_per_aci[aci] = calculated_fee
    
    # Find the maximum fee value across all ACIs
    if not max_fee_per_aci:
        print([])
        return

    global_max_fee = max(max_fee_per_aci.values())
    
    # Identify all ACIs that have this global max fee
    tied_acis = [aci for aci, fee in max_fee_per_aci.items() if fee == global_max_fee]
    
    # Sort alphabetically to handle ties
    tied_acis.sort()
    
    # Return the one with lowest alphabetical order (first in sorted list)
    # Question asks for response in a list
    if tied_acis:
        result = [tied_acis[0]]
    else:
        result = []
        
    print(result)

if __name__ == "__main__":
    analyze_expensive_aci()
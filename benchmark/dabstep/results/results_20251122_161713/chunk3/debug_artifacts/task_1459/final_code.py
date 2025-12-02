import pandas as pd
import json

# Helper functions for robust data processing
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if not v:
            return 0.0
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except ValueError:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

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

def analyze_most_expensive_aci():
    # Load the fees data
    file_path = '/output/chunk3/data/context/fees.json'
    with open(file_path, 'r') as f:
        fees_data = json.load(f)

    # Constants defined in the question
    TRANSACTION_AMOUNT = 10000.0
    TARGET_SCHEME = 'GlobalCard'
    
    # Standard ACIs defined in the manual/dataset
    ALL_ACIS = ['A', 'B', 'C', 'D', 'E', 'F', 'G']

    # Dictionary to track the maximum fee found for each ACI
    # Initialize with -1.0 to ensure we capture any valid positive fee
    aci_max_fees = {aci: -1.0 for aci in ALL_ACIS}

    # Iterate through all fee rules to find the worst-case cost for each ACI
    for rule in fees_data:
        # 1. Filter by Card Scheme
        if rule.get('card_scheme') != TARGET_SCHEME:
            continue
            
        # 2. Filter by Credit Status
        # The question specifies a "credit transaction".
        # Rules apply if is_credit is True (explicit match) OR None (wildcard).
        # We exclude rules where is_credit is explicitly False (Debit rules).
        if rule.get('is_credit') is False:
            continue

        # 3. Calculate Fee for this rule
        # Formula: fee = fixed_amount + rate * transaction_value / 10000
        fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
        rate = coerce_to_float(rule.get('rate', 0))
        
        # Calculate fee for 10,000 EUR
        calculated_fee = fixed_amount + (rate * TRANSACTION_AMOUNT / 10000.0)
        
        # 4. Determine Applicable ACIs for this rule
        # If 'aci' is None or empty list, it applies to ALL ACIs (Wildcard).
        # Otherwise, it applies only to the specific ACIs listed.
        rule_acis = rule.get('aci')
        applicable_acis = []
        
        if not is_not_empty(rule_acis):
            applicable_acis = ALL_ACIS
        else:
            applicable_acis = rule_acis
            
        # 5. Update Max Fee for each applicable ACI
        # We are looking for the "most expensive" scenario, so we track the max fee seen.
        for aci in applicable_acis:
            # Only consider valid ACIs (ignore typos in json if any)
            if aci in aci_max_fees:
                if calculated_fee > aci_max_fees[aci]:
                    aci_max_fees[aci] = calculated_fee

    # Find the highest fee value across all ACIs
    if not aci_max_fees:
        print([])
        return

    overall_max_fee = max(aci_max_fees.values())
    
    # Identify all ACIs that have this maximum fee
    most_expensive_acis = [aci for aci, fee in aci_max_fees.items() if fee == overall_max_fee]
    
    # Sort alphabetically to handle ties as requested ("lowest alphabetical order")
    most_expensive_acis.sort()
    
    # Return the first one as a list
    if most_expensive_acis:
        result = [most_expensive_acis[0]]
        print(result)
    else:
        print([])

if __name__ == "__main__":
    analyze_most_expensive_aci()
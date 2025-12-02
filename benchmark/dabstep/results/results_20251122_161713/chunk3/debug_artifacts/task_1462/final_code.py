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

# Main execution
def main():
    fees_file_path = '/output/chunk3/data/context/fees.json'
    
    try:
        with open(fees_file_path, 'r') as f:
            fees_data = json.load(f)
    except Exception as e:
        print(f"Error loading fees.json: {e}")
        return

    # Constants from question
    TARGET_SCHEME = 'TransactPlus'
    TARGET_AMOUNT = 10000.0
    # ACIs from manual/data (A through G)
    ALL_ACIS = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    
    # Dictionary to store the maximum fee found for each ACI
    # We want to find the "most expensive" scenario for each ACI
    aci_max_fees = {aci: 0.0 for aci in ALL_ACIS}
    
    # Iterate through all fee rules
    for rule in fees_data:
        # 1. Filter by Card Scheme
        # Must match TransactPlus
        if rule.get('card_scheme') != TARGET_SCHEME:
            continue
            
        # 2. Filter by Credit Status
        # Question specifies "credit transaction".
        # Rule applies if is_credit is True (explicitly credit) OR None (wildcard/any)
        # Rule does NOT apply if is_credit is False (explicitly debit/non-credit)
        if rule.get('is_credit') is False:
            continue
            
        # 3. Calculate Fee for this rule
        # Formula: fee = fixed_amount + rate * transaction_value / 10000
        try:
            fixed = coerce_to_float(rule.get('fixed_amount', 0))
            rate = coerce_to_float(rule.get('rate', 0))
            
            # Calculate fee
            fee = fixed + (rate * TARGET_AMOUNT / 10000.0)
        except Exception as e:
            # Skip malformed rules
            continue
            
        # 4. Determine which ACIs this rule applies to
        rule_acis = rule.get('aci')
        applicable_acis = []
        
        if is_not_empty(rule_acis):
            # Rule applies to specific ACIs listed in the rule
            applicable_acis = rule_acis
        else:
            # Rule applies to ALL ACIs (Wildcard)
            applicable_acis = ALL_ACIS
            
        # 5. Update max fee for the applicable ACIs
        # We track the highest possible fee for each ACI to determine which is "most expensive"
        for aci in applicable_acis:
            # Only consider valid ACIs (A-G)
            if aci in aci_max_fees:
                if fee > aci_max_fees[aci]:
                    aci_max_fees[aci] = fee

    # Find the maximum fee across all ACIs
    if not aci_max_fees:
        print([])
        return

    max_fee_value = max(aci_max_fees.values())
    
    # Identify all ACIs that share this maximum fee
    most_expensive_acis = [aci for aci, fee in aci_max_fees.items() if fee == max_fee_value]
    
    # Sort alphabetically to handle ties (lowest alphabetical order)
    most_expensive_acis.sort()
    
    # Return the first one as a list (lowest alphabetical order)
    # Question asks for "the ACI" (singular concept) but "Provide the response in a list"
    result = [most_expensive_acis[0]] if most_expensive_acis else []
    
    print(result)

if __name__ == "__main__":
    main()
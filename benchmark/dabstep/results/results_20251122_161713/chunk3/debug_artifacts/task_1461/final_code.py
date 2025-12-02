import json
import pandas as pd

# Helper function for robust float conversion
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
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def execute_step():
    # Load the fees.json file
    file_path = '/output/chunk3/data/context/fees.json'
    try:
        with open(file_path, 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError:
        print("Error: fees.json not found.")
        return

    # Transaction details
    TRANSACTION_AMOUNT = 10000.0
    TARGET_SCHEME = 'SwiftCharge'
    ALL_ACIS = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    
    # Dictionary to track the maximum fee found for each ACI
    aci_max_fees = {aci: 0.0 for aci in ALL_ACIS}
    
    # Iterate through all fee rules
    for rule in fees_data:
        # 1. Filter by Card Scheme
        if rule.get('card_scheme') != TARGET_SCHEME:
            continue
            
        # 2. Filter by Credit/Debit status
        # We are looking for Credit transactions.
        # Keep rule if is_credit is True (Explicit Match) or None (Wildcard).
        # Discard if is_credit is False (Explicit Mismatch, i.e., Debit only).
        if rule.get('is_credit') is False:
            continue
        
        # 3. Calculate Fee
        # Formula: fee = fixed_amount + rate * (amount / 10000)
        fixed = coerce_to_float(rule.get('fixed_amount'))
        rate = coerce_to_float(rule.get('rate'))
        
        # Since amount is 10,000, rate * (10000/10000) = rate
        calculated_fee = fixed + rate
        
        # 4. Determine Applicable ACIs
        rule_acis = rule.get('aci')
        
        applicable_acis = []
        if not rule_acis: # None or Empty List [] implies Wildcard (All ACIs)
            applicable_acis = ALL_ACIS
        elif isinstance(rule_acis, list):
            applicable_acis = rule_acis
        else:
            # Fallback for unexpected format
            continue
            
        # 5. Update Max Fee for each applicable ACI
        for aci in applicable_acis:
            if aci in aci_max_fees:
                if calculated_fee > aci_max_fees[aci]:
                    aci_max_fees[aci] = calculated_fee

    # Find the maximum fee value across all ACIs
    if not aci_max_fees:
        print("[]")
        return

    max_fee_value = max(aci_max_fees.values())
    
    # Identify all ACIs that share this maximum fee
    most_expensive_acis = [aci for aci, fee in aci_max_fees.items() if fee == max_fee_value]
    
    # Sort alphabetically to handle ties
    most_expensive_acis.sort()
    
    # Return the first one (lowest alphabetical order) as a list
    result = [most_expensive_acis[0]]
    
    print(result)

if __name__ == "__main__":
    execute_step()
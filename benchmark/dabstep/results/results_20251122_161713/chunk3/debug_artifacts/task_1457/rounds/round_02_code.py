# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1457
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 4166 characters (FULL CODE)
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

def main():
    # Load fees data
    fees_file_path = '/output/chunk3/data/context/fees.json'
    try:
        with open(fees_file_path, 'r') as f:
            fees_data = json.load(f)
    except Exception as e:
        print(f"Error loading fees file: {e}")
        return

    # Define transaction parameters based on the question
    # "For a credit transaction of 1000 euros on SwiftCharge..."
    TRANSACTION_AMOUNT = 1000.0
    CARD_SCHEME = 'SwiftCharge'
    IS_CREDIT = True
    ALL_ACIS = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    
    # Dictionary to track the maximum fee found for each ACI
    # Initialize with -1.0 to represent no applicable rule found yet
    max_fee_per_aci = {aci: -1.0 for aci in ALL_ACIS}
    
    # Iterate through all fee rules to find the most expensive scenario for each ACI
    for rule in fees_data:
        # 1. Filter by Card Scheme
        if rule.get('card_scheme') != CARD_SCHEME:
            continue
            
        # 2. Filter by Credit status
        # Rule applies if is_credit matches the transaction (True) OR is None (Wildcard)
        rule_is_credit = rule.get('is_credit')
        if rule_is_credit is not None and rule_is_credit != IS_CREDIT:
            continue
            
        # 3. Calculate Fee for this rule
        try:
            fixed_amount = coerce_to_float(rule.get('fixed_amount', 0))
            rate = coerce_to_float(rule.get('rate', 0))
            
            # Fee formula: fixed + (rate * amount / 10000)
            fee = fixed_amount + (rate * TRANSACTION_AMOUNT / 10000)
        except Exception as e:
            # Skip malformed rules
            continue
            
        # 4. Apply fee to relevant ACIs
        rule_acis = rule.get('aci')
        
        if not is_not_empty(rule_acis):
            # Wildcard ACI: Applies to ALL ACIs
            for aci in ALL_ACIS:
                if fee > max_fee_per_aci[aci]:
                    max_fee_per_aci[aci] = fee
        else:
            # Specific ACIs: Applies only to those listed in the rule
            for aci in rule_acis:
                if aci in max_fee_per_aci:
                    if fee > max_fee_per_aci[aci]:
                        max_fee_per_aci[aci] = fee

    # Find the highest fee among all ACIs
    highest_fee = -1.0
    for fee in max_fee_per_aci.values():
        if fee > highest_fee:
            highest_fee = fee
            
    # Identify ACIs with the highest fee
    # Only consider ACIs that had at least one applicable rule (fee != -1.0)
    most_expensive_acis = [
        aci for aci, fee in max_fee_per_aci.items() 
        if fee == highest_fee and fee != -1.0
    ]
    
    # Sort alphabetically to handle ties as per instructions:
    # "In the case of a draw... return the ACI with the lowest alphabetical order."
    most_expensive_acis.sort()
    
    # Return the result as a list with the single winner
    if most_expensive_acis:
        result = [most_expensive_acis[0]]
    else:
        result = []
        
    print(result)

if __name__ == "__main__":
    main()

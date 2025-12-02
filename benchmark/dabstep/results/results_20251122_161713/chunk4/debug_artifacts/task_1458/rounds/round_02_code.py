# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1458
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3915 characters (FULL CODE)
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

def calculate_fee(amount, rule):
    """Calculate fee based on fixed amount and rate."""
    fixed = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000)

def main():
    # Define the file path for fees.json
    fees_path = '/output/chunk4/data/context/fees.json'

    # Load the JSON data
    try:
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError:
        print("Error: fees.json not found.")
        return

    # Constants for the scenario defined in the question
    TRANSACTION_AMOUNT = 1000.0
    TARGET_SCHEME = 'TransactPlus'
    IS_CREDIT_TRANSACTION = True  # "credit transaction"
    
    # All possible ACIs as defined in manual.md
    ALL_ACIS = ['A', 'B', 'C', 'D', 'E', 'F', 'G']

    # Dictionary to store the maximum potential fee found for each ACI
    # We initialize with 0.0
    aci_max_fees = {aci: 0.0 for aci in ALL_ACIS}

    # Iterate through all fee rules to find applicable ones
    for rule in fees_data:
        # 1. Filter by Card Scheme
        if rule.get('card_scheme') != TARGET_SCHEME:
            continue
        
        # 2. Filter by Credit/Debit status
        # The transaction is Credit.
        # Rule applies if rule['is_credit'] is True (Credit only) OR None (Any/Wildcard).
        # Rule does NOT apply if rule['is_credit'] is False (Debit only).
        rule_is_credit = rule.get('is_credit')
        if rule_is_credit is False:
            continue
            
        # 3. Calculate the fee for this rule
        fee = calculate_fee(TRANSACTION_AMOUNT, rule)
        
        # 4. Determine which ACIs this rule applies to
        rule_acis = rule.get('aci')
        
        if rule_acis is None:
            # Wildcard: Applies to ALL ACIs
            applicable_acis = ALL_ACIS
        else:
            # Specific list of ACIs
            applicable_acis = rule_acis
            
        # 5. Update max fee for applicable ACIs
        # We are looking for the "most expensive" scenario, so we track the max fee per ACI
        for aci in applicable_acis:
            # Only consider valid ACIs (A-G)
            if aci in aci_max_fees:
                if fee > aci_max_fees[aci]:
                    aci_max_fees[aci] = fee

    # Find the maximum fee value across all ACIs
    if not aci_max_fees:
        print([])
        return

    max_fee_value = max(aci_max_fees.values())
    
    # Identify all ACIs that have this maximum fee
    most_expensive_acis = [aci for aci, fee in aci_max_fees.items() if fee == max_fee_value]
    
    # Sort alphabetically to handle ties
    most_expensive_acis.sort()
    
    # Requirement: "In the case of a draw between multiple ACIs, return the ACI with the lowest alphabetical order."
    # Requirement: "Provide the response in a list even if there is only one value."
    if most_expensive_acis:
        result = [most_expensive_acis[0]]
        print(result)
    else:
        print([])

if __name__ == "__main__":
    main()

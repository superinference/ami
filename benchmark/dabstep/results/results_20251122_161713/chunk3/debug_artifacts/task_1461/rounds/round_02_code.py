# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1461
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3308 characters (FULL CODE)
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

def execute_step():
    # Load the fees.json file
    file_path = '/output/chunk3/data/context/fees.json'
    with open(file_path, 'r') as f:
        fees_data = json.load(f)
    
    # Constants
    TRANSACTION_AMOUNT = 10000.0
    ALL_ACIS = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    TARGET_SCHEME = 'SwiftCharge'
    
    # Dictionary to track max fee for each ACI
    # Initialize with 0.0
    aci_max_fees = {aci: 0.0 for aci in ALL_ACIS}
    
    # Filter and Process
    for rule in fees_data:
        # 1. Check Scheme
        if rule.get('card_scheme') != TARGET_SCHEME:
            continue
            
        # 2. Check Credit
        # is_credit can be True, False, or None (Wildcard)
        # We want True or None (applies to credit)
        rule_is_credit = rule.get('is_credit')
        if rule_is_credit is False: # Explicitly Debit, so skip
            continue
        
        # 3. Calculate Fee
        # fee = fixed_amount + rate * transaction_value / 10000
        fixed = coerce_to_float(rule.get('fixed_amount', 0))
        rate = coerce_to_float(rule.get('rate', 0))
        
        # Calculate fee for 10,000 EUR
        fee = fixed + (rate * TRANSACTION_AMOUNT / 10000.0)
        
        # 4. Apply to ACIs
        rule_acis = rule.get('aci')
        
        applicable_acis = []
        if rule_acis is None: # Wildcard (None)
            applicable_acis = ALL_ACIS
        elif isinstance(rule_acis, list):
            if len(rule_acis) == 0: # Empty list treated as wildcard
                 applicable_acis = ALL_ACIS
            else:
                applicable_acis = rule_acis
        else:
            # Unexpected type, skip
            continue
            
        # Update max fees for applicable ACIs
        for aci in applicable_acis:
            if aci in aci_max_fees: # Only consider valid ACIs A-G
                if fee > aci_max_fees[aci]:
                    aci_max_fees[aci] = fee
                    
    # Find the max fee across all ACIs
    if not aci_max_fees:
        print("[]")
        return

    max_fee_value = max(aci_max_fees.values())
    
    # Find all ACIs with this max fee
    most_expensive_acis = [aci for aci, fee in aci_max_fees.items() if fee == max_fee_value]
    
    # Sort alphabetically
    most_expensive_acis.sort()
    
    # Tie-breaker: return the ACI with the lowest alphabetical order
    # Return as a list
    result = [most_expensive_acis[0]]
    
    print(result)

if __name__ == "__main__":
    execute_step()

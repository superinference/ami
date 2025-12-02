# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1445
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 3575 characters (FULL CODE)
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
    # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
    return fixed + (rate * amount / 10000)

# Load the fees.json file
file_path = '/output/chunk4/data/context/fees.json'
with open(file_path, 'r') as f:
    fees_data = json.load(f)

# Parameters from the question
TRANSACTION_AMOUNT = 1.0
TARGET_SCHEME = 'SwiftCharge'
IS_CREDIT_TX = True
# Possible ACIs from manual/data
ALL_ACIS = ['A', 'B', 'C', 'D', 'E', 'F', 'G']

# Dictionary to store max fee for each ACI
# Initialize with -1 to ensure we capture any positive fee
aci_max_fees = {aci: -1.0 for aci in ALL_ACIS}

# Iterate through all fee rules
for rule in fees_data:
    # 1. Filter by Card Scheme
    if rule.get('card_scheme') != TARGET_SCHEME:
        continue
        
    # 2. Filter by Credit Status
    # The transaction is Credit.
    # Rule applies if rule['is_credit'] is True (explicit match) or None (wildcard).
    # If rule['is_credit'] is False, it applies to Debit only, so we skip.
    rule_is_credit = rule.get('is_credit')
    if rule_is_credit is not None and rule_is_credit != IS_CREDIT_TX:
        continue
        
    # 3. Calculate Fee for this rule
    # Since we want the "most expensive" possible scenario, we consider this rule
    # regardless of other specific constraints (MCC, volume, etc.) as the question
    # implies finding the potential maximum cost for this transaction type.
    fee = calculate_fee(TRANSACTION_AMOUNT, rule)
    
    # 4. Identify applicable ACIs for this rule
    rule_acis = rule.get('aci')
    applicable_acis = []
    
    if rule_acis and isinstance(rule_acis, list) and len(rule_acis) > 0:
        # Rule applies to specific ACIs
        applicable_acis = rule_acis
    else:
        # Rule applies to ALL ACIs (wildcard)
        applicable_acis = ALL_ACIS
        
    # 5. Update max fee for applicable ACIs
    for aci in applicable_acis:
        # Only track known ACIs
        if aci in aci_max_fees:
            if fee > aci_max_fees[aci]:
                aci_max_fees[aci] = fee

# Find the global maximum fee across all ACIs
max_fee_value = max(aci_max_fees.values())

# Identify all ACIs that have this maximum fee
most_expensive_acis = [aci for aci, fee in aci_max_fees.items() if fee == max_fee_value]

# Sort alphabetically to handle ties (lowest alphabetical order)
most_expensive_acis.sort()

# Select the winner (lowest alphabetical order)
# The question asks for a list response even if there is only one value
if most_expensive_acis:
    result = [most_expensive_acis[0]]
else:
    result = []

print(result)

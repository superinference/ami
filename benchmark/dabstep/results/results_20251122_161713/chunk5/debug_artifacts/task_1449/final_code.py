import pandas as pd
import json

# Helper function to coerce values to float (handling currency, %, etc.)
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
    """Calculates fee based on fixed amount and rate."""
    fixed = coerce_to_float(rule.get('fixed_amount', 0))
    rate = coerce_to_float(rule.get('rate', 0))
    # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
    return fixed + (rate * amount / 10000)

# 1. Load Data
fees_path = '/output/chunk5/data/context/fees.json'
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Define Constants
TRANSACTION_AMOUNT = 10.0
TARGET_SCHEME = 'SwiftCharge'
# ACIs A through G based on manual.md and payments.csv
ALL_ACIS = ['A', 'B', 'C', 'D', 'E', 'F', 'G']

# 3. Filter Rules
# We need rules that match SwiftCharge AND (Credit OR Wildcard)
applicable_rules = []
for rule in fees_data:
    # Check Scheme
    if rule.get('card_scheme') != TARGET_SCHEME:
        continue
    
    # Check Credit Status
    # is_credit: True (Credit), False (Debit), None (Both/Wildcard)
    # We want Credit (True) or Both (None)
    r_credit = rule.get('is_credit')
    if r_credit is not None and r_credit is not True:
        continue  # Skip if explicitly False (Debit only)
        
    applicable_rules.append(rule)

# 4. Calculate Max Fee per ACI
# Initialize max fee tracker
max_fee_per_aci = {aci: 0.0 for aci in ALL_ACIS}

for rule in applicable_rules:
    # Calculate fee for this specific rule
    fee = calculate_fee(TRANSACTION_AMOUNT, rule)
    
    # Determine which ACIs this rule applies to
    rule_acis = rule.get('aci')
    
    if rule_acis is None:
        # Wildcard: Applies to ALL ACIs
        for aci in ALL_ACIS:
            if fee > max_fee_per_aci[aci]:
                max_fee_per_aci[aci] = fee
    else:
        # Specific List: Applies only to listed ACIs
        for aci in rule_acis:
            if aci in max_fee_per_aci: # Ensure valid ACI
                if fee > max_fee_per_aci[aci]:
                    max_fee_per_aci[aci] = fee

# 5. Determine Winner
# Find the global maximum fee value
highest_fee_value = -1.0
for fee in max_fee_per_aci.values():
    if fee > highest_fee_value:
        highest_fee_value = fee

# Find all ACIs that have this highest fee
expensive_acis = [aci for aci, fee in max_fee_per_aci.items() if fee == highest_fee_value]

# Sort alphabetically to handle ties
expensive_acis.sort()

# Select the winner (lowest alphabetical order)
# The requirement is to return a list even if it's a single value
result = [expensive_acis[0]] if expensive_acis else []

# 6. Output
print(result)
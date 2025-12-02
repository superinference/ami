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

# Load the fees.json file
file_path = '/output/chunk5/data/context/fees.json'
with open(file_path, 'r') as f:
    fees_data = json.load(f)

# Constants for the scenario
TRANSACTION_AMOUNT = 100.0
TARGET_SCHEME = 'GlobalCard'
TARGET_IS_CREDIT = True
ALL_ACIS = ['A', 'B', 'C', 'D', 'E', 'F', 'G']

# Step 1: Filter for rules applicable to GlobalCard and Credit transactions
# A rule is relevant if:
# 1. card_scheme matches TARGET_SCHEME
# 2. is_credit matches TARGET_IS_CREDIT OR is_credit is None (wildcard)
relevant_rules = []
for rule in fees_data:
    # Check Scheme
    if rule.get('card_scheme') != TARGET_SCHEME:
        continue
    
    # Check Credit Status
    rule_credit = rule.get('is_credit')
    # If rule specifies a credit status, it must match True. If None, it applies to both.
    if rule_credit is not None and rule_credit != TARGET_IS_CREDIT:
        continue
        
    relevant_rules.append(rule)

# Step 2: Calculate cost for each ACI
aci_costs = {}

for aci in ALL_ACIS:
    # Find rules applicable to this specific ACI
    # Rule applies if rule['aci'] contains the specific ACI OR rule['aci'] is wildcard (None or empty)
    matching_rules = []
    for rule in relevant_rules:
        rule_acis = rule.get('aci')
        if not is_not_empty(rule_acis): # Wildcard rule (applies to all ACIs)
            matching_rules.append(rule)
        elif aci in rule_acis: # Specific rule (applies to this ACI)
            matching_rules.append(rule)
    
    if not matching_rules:
        # Fallback if no rules found (should not happen in this dataset)
        aci_costs[aci] = 0.0
        continue

    # Logic for selecting the active rule:
    # Specific rules (explicitly listing the ACI) usually override generic/wildcard rules.
    specific_rules = [r for r in matching_rules if is_not_empty(r.get('aci'))]
    wildcard_rules = [r for r in matching_rules if not is_not_empty(r.get('aci'))]
    
    # Prioritize specific rules. If none, use wildcard rules.
    rules_to_consider = specific_rules if specific_rules else wildcard_rules
    
    # Calculate fee for the applicable rule(s). 
    # If multiple specific rules exist (unlikely/ambiguous), we take the max fee to represent "most expensive".
    max_fee_for_this_aci = -1.0
    
    for rule in rules_to_consider:
        fixed = coerce_to_float(rule.get('fixed_amount', 0))
        rate = coerce_to_float(rule.get('rate', 0))
        
        # Fee Calculation: fixed_amount + (rate * transaction_value / 10000)
        fee = fixed + (rate * TRANSACTION_AMOUNT / 10000.0)
        
        if fee > max_fee_for_this_aci:
            max_fee_for_this_aci = fee
            
    aci_costs[aci] = max_fee_for_this_aci

# Step 3: Find the most expensive ACI
if not aci_costs:
    print([])
else:
    # Identify the maximum cost
    max_cost = max(aci_costs.values())
    
    # Find all ACIs that result in this maximum cost (handling floating point precision)
    expensive_acis = [aci for aci, cost in aci_costs.items() if abs(cost - max_cost) < 1e-9]
    
    # Sort alphabetically to handle ties
    expensive_acis.sort()
    
    # Select the first one (lowest alphabetical order)
    result = [expensive_acis[0]]
    
    # Output the result
    print(result)
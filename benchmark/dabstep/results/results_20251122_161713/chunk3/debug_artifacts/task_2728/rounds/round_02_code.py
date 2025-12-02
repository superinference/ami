# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2728
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7978 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m suffixes to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        
        # Handle percentages
        if '%' in v:
            return float(v.replace('%', '')) / 100
            
        # Handle suffixes k (thousands) and m (millions)
        multiplier = 1
        if v.lower().endswith('k'):
            multiplier = 1_000
            v = v[:-1]
        elif v.lower().endswith('m'):
            multiplier = 1_000_000
            v = v[:-1]
            
        try:
            # Handle ranges (e.g., "50-60") - return mean for coercion, 
            # but specific range logic handles the matching elsewhere
            if '-' in v:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            return float(v) * multiplier
        except ValueError:
            return 0.0
    return 0.0

def parse_range_check(value, rule_string):
    """
    Check if a numeric value fits within a rule string like '100k-1m', '>5', '<3.5%'.
    """
    if rule_string is None:
        return True
        
    s = str(rule_string).strip().lower()
    
    # Handle Greater Than
    if s.startswith('>'):
        limit = coerce_to_float(s[1:])
        return value > limit
        
    # Handle Less Than
    if s.startswith('<'):
        limit = coerce_to_float(s[1:])
        return value < limit
        
    # Handle Ranges (e.g., "100k-1m", "7.7%-8.3%")
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            lower = coerce_to_float(parts[0])
            upper = coerce_to_float(parts[1])
            return lower <= value <= upper
            
    # Handle Exact Match (rare for these fields, but possible)
    try:
        target = coerce_to_float(s)
        return value == target
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Check if a transaction context matches a fee rule.
    tx_ctx: dict containing transaction details and merchant monthly stats
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match required)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List membership or Wildcard)
    # If rule has list, tx value must be in it. If rule is empty/null, it's a wildcard.
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List membership or Wildcard)
    if rule.get('merchant_category_code'):
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Exact match or Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 5. ACI (List membership or Wildcard)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Exact match or Wildcard)
    if rule.get('intracountry') is not None:
        # tx_ctx['intracountry'] is boolean (True/False)
        # rule['intracountry'] is 1.0/0.0 or True/False
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False

    # 7. Monthly Volume (Range check or Wildcard)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 8. Monthly Fraud Level (Range check or Wildcard)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Define Scope
target_merchant = 'Crossfit_Hanna'
july_start = 182
july_end = 212

# 3. Get Merchant Metadata (Account Type, MCC)
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

account_type = merchant_info['account_type']
mcc = merchant_info['merchant_category_code']

# 4. Calculate Monthly Stats for July (Critical for Fee Tiers)
# Filter for ALL July transactions for this merchant
july_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['day_of_year'] >= july_start) &
    (df_payments['day_of_year'] <= july_end)
]

monthly_volume = july_txs['eur_amount'].sum()
fraud_volume = july_txs[july_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()

# Calculate fraud rate (ratio)
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

print(f"Merchant: {target_merchant}")
print(f"July Volume: €{monthly_volume:,.2f}")
print(f"July Fraud Rate: {monthly_fraud_rate:.2%}")

# 5. Isolate Target Transactions (The Fraudulent Ones)
# We want to see the cost of THESE specific transactions if we moved them to a different ACI
target_fraud_txs = july_txs[july_txs['has_fraudulent_dispute'] == True].copy()
print(f"Target Transactions Count: {len(target_fraud_txs)}")

# 6. Simulate Fees for Each ACI
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
aci_costs = {}

for test_aci in possible_acis:
    total_fee_for_aci = 0.0
    
    for _, tx in target_fraud_txs.iterrows():
        # Determine if transaction is intracountry
        is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
        
        # Create context for this specific transaction + merchant stats + TEST ACI
        context = {
            'card_scheme': tx['card_scheme'],
            'account_type': account_type,
            'merchant_category_code': mcc,
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_rate,
            'is_credit': bool(tx['is_credit']),
            'aci': test_aci,  # <--- The variable we are simulating
            'intracountry': is_intracountry
        }
        
        # Find the first matching fee rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(context, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            # Fee = Fixed + (Rate * Amount / 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000)
            total_fee_for_aci += fee
        else:
            # If no rule matches, this ACI might be invalid for this card/type combination.
            # We treat this as a high cost or skip, but for this problem, we assume coverage exists.
            # Adding a penalty to discourage selection if coverage is missing.
            total_fee_for_aci += 9999.0 

    aci_costs[test_aci] = total_fee_for_aci

# 7. Determine Preferred Choice
# Sort by cost ascending
sorted_costs = sorted(aci_costs.items(), key=lambda x: x[1])
best_aci, lowest_cost = sorted_costs[0]

print("\n--- Simulation Results ---")
for aci, cost in sorted_costs:
    print(f"ACI {aci}: €{cost:,.2f}")

print(f"\nPreferred ACI: {best_aci}")

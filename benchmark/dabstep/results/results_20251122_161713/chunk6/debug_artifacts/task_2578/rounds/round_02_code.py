# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2578
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8718 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS (Robust Data Processing)
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v:
            return float(v.replace('k', '')) * 1000
        if 'm' in v:
            return float(v.replace('m', '')) * 1000000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range(range_str):
    """Parses range strings like '100k-1m', '>5', '<3', '0%-1%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower()
    
    # Handle greater/less than
    if s.startswith('>'):
        return coerce_to_float(s[1:]), float('inf')
    if s.startswith('<'):
        return float('-inf'), coerce_to_float(s[1:])
        
    # Handle ranges
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            return coerce_to_float(parts[0]), coerce_to_float(parts[1])
            
    # Handle exact match (rare for ranges, but possible)
    val = coerce_to_float(s)
    return val, val

def check_range_match(value, rule_range_str):
    """Checks if a numeric value falls within a rule's string range."""
    if rule_range_str is None:  # Wildcard
        return True
    min_val, max_val = parse_range(rule_range_str)
    if min_val is None:
        return False
    # Handle edge case where value is exactly on the boundary? 
    # Usually ranges are inclusive, but let's assume standard inclusive logic
    return min_val <= value <= max_val

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    Returns True if the rule applies, False otherwise.
    """
    # 1. Card Scheme (Must match exactly for this simulation)
    if rule.get('card_scheme') != tx_ctx.get('target_scheme'):
        return False

    # 2. Account Type (List membership or Empty=All)
    if rule.get('account_type') and tx_ctx.get('account_type') not in rule['account_type']:
        return False

    # 3. Merchant Category Code (List membership or Empty=All)
    if rule.get('merchant_category_code') and tx_ctx.get('mcc') not in rule['merchant_category_code']:
        return False

    # 4. Capture Delay (Exact match or None=All)
    if rule.get('capture_delay') is not None and rule['capture_delay'] != tx_ctx.get('capture_delay'):
        return False

    # 5. Monthly Volume (Range check or None=All)
    if rule.get('monthly_volume') is not None:
        if not check_range_match(tx_ctx.get('monthly_volume'), rule['monthly_volume']):
            return False

    # 6. Monthly Fraud Level (Range check or None=All)
    if rule.get('monthly_fraud_level') is not None:
        if not check_range_match(tx_ctx.get('monthly_fraud_rate'), rule['monthly_fraud_level']):
            return False

    # 7. Is Credit (Boolean match or None=All)
    if rule.get('is_credit') is not None and rule['is_credit'] != tx_ctx.get('is_credit'):
        return False

    # 8. ACI (List membership or Empty=All)
    if rule.get('aci') and tx_ctx.get('aci') not in rule['aci']:
        return False

    # 9. Intracountry (Boolean match or None=All)
    if rule.get('intracountry') is not None:
        # Intracountry is True if Issuer == Acquirer
        is_intra = tx_ctx.get('issuing_country') == tx_ctx.get('acquirer_country')
        # Rule expects True/False/0.0/1.0
        rule_intra = bool(rule['intracountry'])
        if is_intra != rule_intra:
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data_list = json.load(f)

# 2. Filter for Merchant and Timeframe (January)
target_merchant = 'Belles_cookbook_store'
df_jan = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['day_of_year'] >= 1) & 
    (df_payments['day_of_year'] <= 31)
].copy()

# 3. Get Merchant Profile
merchant_info = next((m for m in merchant_data_list if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats (Volume & Fraud)
# These are constant for the merchant regardless of the scheme used for simulation
monthly_volume = df_jan['eur_amount'].sum()
fraud_count = df_jan['has_fraudulent_dispute'].sum()
total_count = len(df_jan)
monthly_fraud_rate = (fraud_count / total_count) if total_count > 0 else 0.0

print(f"Merchant: {target_merchant}")
print(f"Jan Volume: €{monthly_volume:,.2f}")
print(f"Jan Fraud Rate: {monthly_fraud_rate:.2%}")
print(f"MCC: {mcc}, Account: {account_type}, Delay: {capture_delay}")

# 5. Identify Available Schemes
available_schemes = set(rule['card_scheme'] for rule in fees_data)
print(f"Simulating schemes: {available_schemes}")

# 6. Simulation Loop
scheme_costs = {}

for scheme in available_schemes:
    total_scheme_fee = 0.0
    valid_scheme = True
    
    # Iterate through every transaction
    for _, tx in df_jan.iterrows():
        # Build Context
        tx_ctx = {
            'target_scheme': scheme,  # SIMULATION: Pretend tx is this scheme
            'mcc': mcc,
            'account_type': account_type,
            'capture_delay': capture_delay,
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate,
            'is_credit': bool(tx['is_credit']),
            'aci': tx['aci'],
            'issuing_country': tx['issuing_country'],
            'acquirer_country': tx['acquirer_country'],
            'eur_amount': tx['eur_amount']
        }
        
        # Find matching rule
        # We look for the first rule that matches. 
        # In a real engine, there might be priority, but usually specific matches first.
        # The dataset structure implies a flat search is sufficient if rules are mutually exclusive or ordered.
        # We will assume the first match in the JSON list is the applicable one.
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(tx_ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_scheme_fee += fee
        else:
            # If a scheme has NO rule for a specific transaction type (e.g. specific ACI),
            # it implies the scheme cannot process it. 
            # For this problem, we assume we want the valid scheme with min fees.
            # We'll penalize invalid schemes or just mark them.
            # However, usually there is a fallback. If not found, we can't steer traffic there.
            # Let's assume high cost or skip.
            # print(f"Warning: No rule found for scheme {scheme} and tx {tx['psp_reference']}")
            valid_scheme = False
            break
    
    if valid_scheme:
        scheme_costs[scheme] = total_scheme_fee
        print(f"Scheme: {scheme:<15} | Total Fee: €{total_scheme_fee:,.2f}")
    else:
        print(f"Scheme: {scheme:<15} | Invalid (Missing rules for some txs)")

# 7. Determine Winner
if scheme_costs:
    best_scheme = min(scheme_costs, key=scheme_costs.get)
    min_fee = scheme_costs[best_scheme]
    print(f"\nRecommended Scheme: {best_scheme} (Cost: €{min_fee:,.2f})")
    # The question asks "to which card scheme...". We output just the name.
    print(best_scheme)
else:
    print("No valid schemes found.")

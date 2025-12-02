# ═══════════════════════════════════════════════════════════
# Round 4 - Task 2606
# ═══════════════════════════════════════════════════════════
# Temperature: 0.30
# Verification: sufficient
# Plan steps: 3
# Code length: 7734 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except:
                return 0.0
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except:
            return 0.0
    return 0.0

def parse_range_check(value, rule_string, is_percentage=False):
    """
    Checks if a numeric value fits within a rule string range (e.g., '100k-1m', '>5%', '<3').
    """
    if rule_string is None:
        return True
    
    s = str(rule_string).strip().lower()
    if s == 'none' or s == '':
        return True

    try:
        if '-' in s:
            parts = s.split('-')
            # Clean suffixes like k, m, %
            low_s = parts[0].replace('%', '').replace('k', '000').replace('m', '000000')
            high_s = parts[1].replace('%', '').replace('k', '000').replace('m', '000000')
            low = float(low_s)
            high = float(high_s)
            if is_percentage:
                low /= 100
                high /= 100
            return low <= value <= high
        elif s.startswith('>'):
            limit_s = s[1:].replace('%', '').replace('k', '000').replace('m', '000000')
            limit = float(limit_s)
            if is_percentage: limit /= 100
            return value > limit
        elif s.startswith('<'):
            limit_s = s[1:].replace('%', '').replace('k', '000').replace('m', '000000')
            limit = float(limit_s)
            if is_percentage: limit /= 100
            return value < limit
        else:
            # Exact match
            limit_s = s.replace('%', '').replace('k', '000').replace('m', '000000')
            limit = float(limit_s)
            if is_percentage: limit /= 100
            return value == limit
    except:
        return False

def match_fee_rule(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    """
    # 1. Card Scheme (Explicit match required)
    if rule.get('card_scheme') != tx_context['card_scheme']:
        return False

    # 2. Account Type (Wildcard [] matches all)
    if rule.get('account_type') and len(rule['account_type']) > 0:
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. MCC (Wildcard [] matches all)
    if rule.get('merchant_category_code') and len(rule['merchant_category_code']) > 0:
        if int(tx_context['mcc']) not in [int(x) for x in rule['merchant_category_code']]:
            return False

    # 4. Capture Delay (Wildcard None matches all)
    if rule.get('capture_delay'):
        rd = str(rule['capture_delay'])
        md = str(tx_context['capture_delay'])
        # Handle numeric delays (e.g. "7") vs range rules (e.g. ">5")
        if md.isdigit() or (md.replace('.', '', 1).isdigit()):
             if not parse_range_check(float(md), rd):
                 return False
        else:
             # String match (e.g. "manual")
             if rd != md:
                 return False

    # 5. Monthly Volume
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 6. Monthly Fraud Level
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level'], is_percentage=True):
            return False

    # 7. Is Credit (Wildcard None matches all)
    if rule.get('is_credit') is not None:
        if str(rule['is_credit']).lower() != str(tx_context['is_credit']).lower():
            return False

    # 8. ACI (Wildcard [] matches all)
    if rule.get('aci') and len(rule['aci']) > 0:
        if tx_context['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Wildcard None matches all)
    if rule.get('intracountry') is not None:
        try:
            rule_intra = bool(float(rule['intracountry']))
            if rule_intra != tx_context['intracountry']:
                return False
        except:
            pass 

    return True

def calculate_fee(amount, rule):
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchants = json.load(f)

# 2. Filter for Rafa_AI in March (Day 60-90)
merchant_name = 'Rafa_AI'
df_march = df[
    (df['merchant'] == merchant_name) & 
    (df['day_of_year'] >= 60) & 
    (df['day_of_year'] <= 90)
].copy()

# 3. Calculate Merchant Stats (Volume & Fraud)
monthly_volume = df_march['eur_amount'].sum()
fraud_count = df_march['has_fraudulent_dispute'].sum()
tx_count = len(df_march)
monthly_fraud_rate = fraud_count / tx_count if tx_count > 0 else 0.0

# 4. Get Merchant Static Data
merchant_info = next((m for m in merchants if m['merchant'] == merchant_name), None)
if not merchant_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 5. Identify Available Schemes
schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
scheme_costs = {}

# 6. Simulate Costs for Each Scheme
for scheme in schemes:
    total_fee = 0.0
    match_count = 0
    
    # Pre-filter fees for this scheme to speed up processing
    scheme_rules = [f for f in fees if f['card_scheme'] == scheme]
    
    for _, tx in df_march.iterrows():
        # Determine transaction-specific attributes
        is_credit = tx['is_credit']
        aci = tx['aci']
        # Intracountry: Issuing == Acquirer
        intracountry = (tx['issuing_country'] == tx['acquirer_country'])
        
        # Context for this specific transaction
        context = {
            'card_scheme': scheme,
            'account_type': account_type,
            'mcc': mcc,
            'capture_delay': capture_delay,
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate,
            'is_credit': is_credit,
            'aci': aci,
            'intracountry': intracountry
        }
        
        # Find matching rule
        matched_rule = None
        for rule in scheme_rules:
            if match_fee_rule(context, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fee += fee
            match_count += 1
        else:
            # If we can't find a rule for a transaction, this scheme can't handle the traffic
            break
    
    # Only consider valid if we matched all transactions
    if match_count == len(df_march):
        scheme_costs[scheme] = total_fee

# 7. Find Minimum
if scheme_costs:
    best_scheme = min(scheme_costs, key=scheme_costs.get)
    print(best_scheme)
else:
    print("No valid schemes found")

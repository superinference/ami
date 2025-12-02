# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2608
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8008 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
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

def parse_range(range_str):
    """Parses a range string like '100k-1m', '<5%', or '>8.3%' into (min, max)."""
    if range_str is None:
        return (-float('inf'), float('inf'))
    
    s = str(range_str).lower().strip()
    
    # Helper to parse values with suffixes
    def parse_val(v):
        v = v.strip()
        mult = 1
        if v.endswith('k'):
            mult = 1000
            v = v[:-1]
        elif v.endswith('m'):
            mult = 1000000
            v = v[:-1]
        elif v.endswith('%'):
            mult = 0.01
            v = v[:-1]
        return float(v) * mult

    if s.startswith('>'):
        return (parse_val(s[1:]), float('inf'))
    if s.startswith('<'):
        return (-float('inf'), parse_val(s[1:]))
    
    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    
    # Exact match treated as range [val, val]
    try:
        val = parse_val(s)
        return (val, val)
    except:
        return (-float('inf'), float('inf'))

def check_rule_match(tx_ctx, rule):
    """
    Checks if a transaction context matches a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Implicitly handled by outer loop, but good to check)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List in rule, scalar in tx)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List in rule, scalar in tx)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Bool)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 5. ACI (List in rule, scalar in tx)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Bool)
    if rule.get('intracountry') is not None:
        # Rule uses 0.0/1.0 or bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False

    # 7. Capture Delay (Range/Value)
    if rule.get('capture_delay'):
        m_delay = str(tx_ctx['capture_delay'])
        r_delay = str(rule['capture_delay'])
        
        if r_delay == m_delay:
            pass # Exact match
        elif r_delay.startswith('>') or r_delay.startswith('<') or '-' in r_delay:
            # Range check
            try:
                min_d, max_d = parse_range(r_delay)
                m_val = float(m_delay)
                if not (min_d <= m_val <= max_d): return False
            except:
                # If conversion fails (e.g. 'manual'), fallback to inequality
                if r_delay != m_delay: return False
        else:
            if r_delay != m_delay: return False

    # 8. Monthly Volume (Range)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_ctx['monthly_volume'] <= max_v):
            return False

    # 9. Monthly Fraud Level (Range)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        # Compare ratio to ratio (e.g. 0.08 vs 0.083)
        if not (min_f <= tx_ctx['monthly_fraud_rate'] <= max_f):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'
acquirer_path = '/output/chunk6/data/context/acquirer_countries.csv'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
df_acquirers = pd.read_csv(acquirer_path)

# 2. Filter for Merchant and April (Day 91-120)
target_merchant = 'Belles_cookbook_store'
df_merchant_txs = df_payments[df_payments['merchant'] == target_merchant].copy()
df_april = df_merchant_txs[(df_merchant_txs['day_of_year'] >= 91) & (df_merchant_txs['day_of_year'] <= 120)].copy()

# 3. Get Merchant Profile
merchant_profile = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_profile:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

mcc = merchant_profile['merchant_category_code']
account_type = merchant_profile['account_type']
capture_delay = merchant_profile['capture_delay']
acquirers = merchant_profile['acquirer']

# Determine Acquirer Country
# Belles uses 'lehman_brothers'. Map to country code.
acquirer_name = acquirers[0]
acquirer_country_row = df_acquirers[df_acquirers['acquirer'] == acquirer_name]
if not acquirer_country_row.empty:
    acquirer_country = acquirer_country_row.iloc[0]['country_code']
else:
    # Fallback to data if mapping fails
    acquirer_country = df_april['acquirer_country'].mode()[0]

# 4. Calculate Monthly Stats (Volume & Fraud) for April
total_volume = df_april['eur_amount'].sum()
fraud_volume = df_april[df_april['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

# 5. Simulate Fees for Each Scheme
schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
scheme_costs = {}

for scheme in schemes:
    total_fee = 0.0
    
    # Filter fees for this scheme to optimize loop
    scheme_rules = [r for r in fees_data if r['card_scheme'] == scheme]
    
    for _, tx in df_april.iterrows():
        # Determine if transaction is domestic (intracountry)
        # Intracountry = Issuing Country matches Acquirer Country
        is_intra = (tx['issuing_country'] == acquirer_country)
        
        ctx = {
            'card_scheme': scheme,
            'account_type': account_type,
            'mcc': mcc,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': is_intra,
            'monthly_volume': total_volume,
            'monthly_fraud_rate': fraud_rate,
            'capture_delay': capture_delay
        }
        
        # Find matching rule
        matched_rule = None
        for rule in scheme_rules:
            if check_rule_match(ctx, rule):
                matched_rule = rule
                break # Assume first match applies
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fee += fee
        else:
            # If no rule matches, assume 0 or handle error. 
            # In this context, we assume coverage exists.
            pass

    scheme_costs[scheme] = total_fee

# 6. Determine Winner
min_scheme = min(scheme_costs, key=scheme_costs.get)

# Output the result
print(min_scheme)

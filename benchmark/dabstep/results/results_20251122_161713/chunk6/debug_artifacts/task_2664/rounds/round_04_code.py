# ═══════════════════════════════════════════════════════════
# Round 4 - Task 2664
# ═══════════════════════════════════════════════════════════
# Temperature: 0.30
# Verification: sufficient
# Plan steps: 2
# Code length: 7524 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range_check(value, rule_string):
    """
    Checks if a numeric value fits into a rule string.
    Handles: '100k-1m', '>5', '<3', '7.7%-8.3%', 'immediate'.
    """
    if rule_string is None:
        return True
    
    s = str(rule_string).strip().lower()
    
    # Handle 'immediate' or 'manual' vs numeric days
    if s in ['immediate', 'manual']:
        if isinstance(value, str):
            return s == value.lower()
        return False 
    
    # Convert value to float for numeric comparison
    try:
        if isinstance(value, str):
            if value.lower() == 'immediate': val = 0.0
            elif value.lower() == 'manual': val = 999.0 
            else: val = float(value)
        else:
            val = float(value)
    except:
        return False 

    # Helper to parse rule parts (k, m, %)
    def parse_rule_num(n_str):
        n_str = n_str.strip()
        is_pct = '%' in n_str
        n_str = n_str.replace('%', '')
        mult = 1
        if n_str.endswith('k'):
            mult = 1000
            n_str = n_str[:-1]
        elif n_str.endswith('m'):
            mult = 1000000
            n_str = n_str[:-1]
        
        try:
            v = float(n_str)
            if is_pct: v /= 100.0
            return v * mult
        except:
            return 0.0

    try:
        if '-' in s:
            parts = s.split('-')
            low = parse_rule_num(parts[0])
            high = parse_rule_num(parts[1])
            # Use isclose for float precision issues
            return (low <= val <= high) or np.isclose(val, low) or np.isclose(val, high)
        elif s.startswith('>'):
            limit = parse_rule_num(s[1:])
            return val > limit
        elif s.startswith('<'):
            limit = parse_rule_num(s[1:])
            return val < limit
        else:
            target = parse_rule_num(s)
            return np.isclose(val, target)
    except:
        return False

def match_fee_rule(ctx, rule):
    """
    Matches a transaction context against a fee rule.
    """
    # 1. Card Scheme
    if rule.get('card_scheme') != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List in rule, single in ctx)
    if rule.get('account_type'):
        if ctx['account_type'] not in rule['account_type']:
            return False
        
    # 3. MCC (List in rule, single in ctx)
    if rule.get('merchant_category_code'):
        if ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False
        
    # 4. Capture Delay
    if rule.get('capture_delay') is not None:
        r_delay = str(rule['capture_delay']).lower()
        m_delay = str(ctx['capture_delay']).lower()
        
        if r_delay in ['immediate', 'manual']:
            if m_delay != r_delay:
                return False
        elif m_delay in ['immediate', 'manual']:
            days = 0 if m_delay == 'immediate' else 999
            if not parse_range_check(days, r_delay):
                return False
        else:
            if not parse_range_check(m_delay, r_delay):
                return False

    # 5. Monthly Volume
    if rule.get('monthly_volume') is not None:
        if not parse_range_check(ctx['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 6. Monthly Fraud Level
    if rule.get('monthly_fraud_level') is not None:
        if not parse_range_check(ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    # 7. Is Credit
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False
            
    # 8. ACI
    if rule.get('aci'):
        if ctx['aci'] not in rule['aci']:
            return False
        
    # 9. Intracountry
    if rule.get('intracountry') is not None:
        rule_domestic = bool(rule['intracountry'])
        ctx_domestic = ctx['intracountry']
        if rule_domestic != ctx_domestic:
            return False
            
    return True

# --- Main Execution ---

# Define file paths
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

# Load data
df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# Filter for Martinis in September (Days 244-273)
merchant_name = 'Martinis_Fine_Steakhouse'
start_day = 244
end_day = 273

sept_txs = df[
    (df['merchant'] == merchant_name) & 
    (df['day_of_year'] >= start_day) & 
    (df['day_of_year'] <= end_day)
].copy()

# Get Merchant Metadata
m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not m_info:
    raise ValueError(f"Merchant {merchant_name} not found")

# Calculate Monthly Stats (Volume & Fraud)
# Manual 5.1.4 & 7: Fraud is ratio of fraudulent VOLUME over total VOLUME
monthly_vol = sept_txs['eur_amount'].sum()
fraud_vol = sept_txs[sept_txs['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate = fraud_vol / monthly_vol if monthly_vol > 0 else 0.0

# Define Schemes to Test
schemes = ['GlobalCard', 'NexPay', 'SwiftCharge', 'TransactPlus']
scheme_costs = {}

# Pre-filter fees by scheme for performance
fees_by_scheme = {s: [r for r in fees if r['card_scheme'] == s] for s in schemes}

# Simulate Costs for Each Scheme
for scheme in schemes:
    total_fee = 0.0
    scheme_rules = fees_by_scheme[scheme]
    
    for _, tx in sept_txs.iterrows():
        # Determine Intracountry (Issuer == Acquirer)
        is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
        
        ctx = {
            'card_scheme': scheme,
            'account_type': m_info['account_type'],
            'merchant_category_code': m_info['merchant_category_code'],
            'capture_delay': m_info['capture_delay'],
            'monthly_volume': monthly_vol,
            'monthly_fraud_level': monthly_fraud_rate,
            'is_credit': bool(tx['is_credit']),
            'aci': tx['aci'],
            'intracountry': is_intracountry
        }
        
        # Find Matching Rule (First match wins)
        matched_rule = None
        for rule in scheme_rules:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            # Fee = Fixed + (Rate * Amount / 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000.0)
            total_fee += fee
        else:
            # If no rule matches, apply a high penalty to rule out this scheme
            total_fee += 100.0 
            
    scheme_costs[scheme] = total_fee

# Determine Best Scheme
best_scheme = min(scheme_costs, key=scheme_costs.get)
print(best_scheme)

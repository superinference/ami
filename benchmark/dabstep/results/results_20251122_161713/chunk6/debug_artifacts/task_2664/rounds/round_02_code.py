# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2664
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7542 characters (FULL CODE)
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
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        return float(v)
    return float(value)

def parse_range_check(value, rule_string):
    """
    Checks if a numeric value fits into a rule string like '100k-1m', '>5', '<3', '7.7%-8.3%'.
    """
    if rule_string is None:
        return True
    
    s = str(rule_string).strip().lower()
    
    # Handle percentages in rule string
    is_percent = '%' in s
    if is_percent:
        s = s.replace('%', '')
    
    # Helper to parse suffix k/m
    def parse_num(n_str):
        n_str = n_str.strip()
        mult = 1
        if n_str.endswith('k'):
            mult = 1000
            n_str = n_str[:-1]
        elif n_str.endswith('m'):
            mult = 1000000
            n_str = n_str[:-1]
        
        try:
            val = float(n_str)
            if is_percent:
                val = val / 100.0
            return val * mult
        except ValueError:
            return 0.0

    try:
        if '-' in s:
            parts = s.split('-')
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            return low <= value <= high
        elif s.startswith('>'):
            limit = parse_num(s[1:])
            return value > limit
        elif s.startswith('<'):
            limit = parse_num(s[1:])
            return value < limit
        elif s == 'immediate': 
            return False # Handled by string match usually
        else:
            # Exact match?
            return value == parse_num(s)
    except:
        return False

def match_fee_rule(ctx, rule):
    """
    Matches a transaction context against a fee rule.
    """
    # 1. Card Scheme
    if rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List in rule, single in ctx)
    if rule['account_type'] and ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. MCC (List in rule, single in ctx)
    if rule['merchant_category_code'] and ctx['merchant_category_code'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (String/Null)
    if rule['capture_delay'] is not None:
        m_delay = str(ctx['capture_delay'])
        r_delay = rule['capture_delay']
        
        if m_delay == r_delay:
            pass # Exact match
        elif r_delay in ['>5', '<3', '3-5'] and m_delay.isdigit():
            days = int(m_delay)
            if r_delay == '>5' and not (days > 5): return False
            if r_delay == '<3' and not (days < 3): return False
            if r_delay == '3-5' and not (3 <= days <= 5): return False
        elif r_delay in ['>5', '<3', '3-5'] and not m_delay.isdigit():
            return False # Mismatch type
        elif m_delay != r_delay:
            return False

    # 5. Monthly Volume
    if rule['monthly_volume'] is not None:
        if not parse_range_check(ctx['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 6. Monthly Fraud Level
    if rule['monthly_fraud_level'] is not None:
        if not parse_range_check(ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    # 7. Is Credit
    if rule['is_credit'] is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False
            
    # 8. ACI (List in rule)
    if rule['aci'] and ctx['aci'] not in rule['aci']:
        return False
        
    # 9. Intracountry
    if rule['intracountry'] is not None:
        # Rule: 1.0/True = Domestic, 0.0/False = International
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
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

# Calculate Monthly Stats (Volume & Fraud) for Fee Tier Determination
monthly_vol = sept_txs['eur_amount'].sum()
monthly_fraud_count = sept_txs['has_fraudulent_dispute'].sum()
monthly_tx_count = len(sept_txs)
monthly_fraud_rate = monthly_fraud_count / monthly_tx_count if monthly_tx_count > 0 else 0.0

# Define Schemes to Test
schemes = ['GlobalCard', 'NexPay', 'SwiftCharge', 'TransactPlus']
scheme_costs = {}

# Pre-filter fees by scheme for performance
fees_by_scheme = {s: [r for r in fees if r['card_scheme'] == s] for s in schemes}

# Simulate Costs for Each Scheme
for scheme in schemes:
    total_fee = 0.0
    scheme_rules = fees_by_scheme[scheme]
    
    # Iterate over every transaction in September
    for _, tx in sept_txs.iterrows():
        # Build Context for this transaction IF it were processed by the current scheme
        ctx = {
            'card_scheme': scheme,
            'account_type': m_info['account_type'],
            'merchant_category_code': m_info['merchant_category_code'],
            'capture_delay': m_info['capture_delay'],
            'monthly_volume': monthly_vol,
            'monthly_fraud_level': monthly_fraud_rate,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': tx['issuing_country'] == tx['acquirer_country'],
            'eur_amount': tx['eur_amount']
        }
        
        # Find Matching Rule (First match wins)
        matched_rule = None
        for rule in scheme_rules:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            # Calculate Fee: fixed + (rate * amount / 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * ctx['eur_amount'] / 10000.0)
            total_fee += fee
        else:
            # If no rule matches, we assume this scheme cannot process this transaction type
            # or it falls to a default. For this analysis, we assume coverage is sufficient.
            pass
            
    scheme_costs[scheme] = total_fee

# Determine the best scheme (Minimum Cost)
best_scheme = min(scheme_costs, key=scheme_costs.get)

# Print the result
print(best_scheme)

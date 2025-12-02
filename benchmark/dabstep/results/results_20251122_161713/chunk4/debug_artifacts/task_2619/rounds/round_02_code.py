# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2619
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8192 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
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
        except ValueError:
            return 0.0
    return float(value) if value is not None else 0.0

def parse_range_check(value, range_str):
    """
    Checks if a numeric value falls within a range string (e.g., '100k-1m', '>8.3%', '<3').
    Handles percentages and k/m suffixes.
    """
    if range_str is None:
        return True
        
    s = str(range_str).lower().replace(',', '').replace('€', '').replace('$', '')
    is_pct = '%' in s
    s = s.replace('%', '')
    
    # Helper to parse single value with k/m
    def parse_val(v):
        m = 1.0
        if 'k' in v: m = 1000.0; v = v.replace('k', '')
        if 'm' in v: m = 1000000.0; v = v.replace('m', '')
        try:
            return float(v) * m
        except:
            return 0.0

    # Normalize value if the range was a percentage
    # If range is "0%-0.8%", it parses to 0.0-0.8. 
    # If value is 0.005 (0.5%), we should multiply value by 100 to match 0.5, OR divide range by 100.
    # Let's divide range by 100 to match the raw ratio value (0.005).
    scale = 0.01 if is_pct else 1.0

    if '-' in s:
        parts = s.split('-')
        low = parse_val(parts[0]) * scale
        high = parse_val(parts[1]) * scale
        return low <= value <= high
    elif '>' in s:
        low = parse_val(s.replace('>', '')) * scale
        return value > low
    elif '<' in s:
        high = parse_val(s.replace('<', '')) * scale
        return value < high
    elif 'immediate' in s or 'manual' in s:
        # String match for capture_delay
        return s == str(value).lower()
    else:
        # Exact match numeric
        target = parse_val(s) * scale
        return value == target

def match_fee_rule(ctx, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    ctx: dict containing transaction and merchant details
    rule: dict containing fee rule criteria
    """
    # 1. Card Scheme (Exact Match)
    if rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List Match)
    # Rule field is list or null. Merchant field is string.
    if rule['account_type'] is not None and len(rule['account_type']) > 0:
        if ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List Match)
    # Rule field is list or null. Merchant field is int.
    if rule['merchant_category_code'] is not None and len(rule['merchant_category_code']) > 0:
        if ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (String/Range Match)
    # Rule field is string (range or value) or null. Merchant field is string.
    if rule['capture_delay'] is not None:
        # If merchant has "manual" and rule is "manual", match.
        # If merchant has "manual" and rule is ">5", do NOT match (strings don't compare to ranges easily unless defined).
        # Based on data, capture_delay is categorical ("manual", "immediate") or numeric days.
        if str(rule['capture_delay']) != str(ctx['capture_delay']):
            # Try range parsing if both look numeric? 
            # Merchant "manual" won't match ">5".
            return False
            
    # 5. Monthly Volume (Range Match)
    if not parse_range_check(ctx['monthly_volume'], rule['monthly_volume']):
        return False
        
    # 6. Monthly Fraud Level (Range Match)
    if not parse_range_check(ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
        return False
        
    # 7. Is Credit (Bool Match)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False
            
    # 8. ACI (List Match)
    if rule['aci'] is not None and len(rule['aci']) > 0:
        if ctx['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry (Bool Match)
    if rule['intracountry'] is not None:
        # Intracountry is True if Issuer == Acquirer
        # fees.json uses 0.0 for False? Or boolean?
        # Sample shows "intracountry": 0.0 or null. 0.0 likely means False.
        # Let's handle 0.0/1.0/True/False
        rule_intra = bool(rule['intracountry'])
        if rule_intra != ctx['intracountry']:
            return False
            
    return True

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

# Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 1. Filter for Crossfit_Hanna in May (Days 121-151)
merchant_name = 'Crossfit_Hanna'
df_merchant = df[df['merchant'] == merchant_name].copy()
df_may = df_merchant[(df_merchant['day_of_year'] >= 121) & (df_merchant['day_of_year'] <= 151)].copy()

# 2. Calculate Merchant Stats for May
# Volume
monthly_volume = df_may['eur_amount'].sum()

# Fraud Rate (Fraud Volume / Total Volume)
fraud_txs = df_may[df_may['has_fraudulent_dispute'] == True]
fraud_volume = fraud_txs['eur_amount'].sum()
monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# Get Merchant Static Data
m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not m_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

# 3. Simulate Fees for Each Scheme
schemes = ['GlobalCard', 'NexPay', 'SwiftCharge', 'TransactPlus']
scheme_totals = {}

for scheme in schemes:
    total_fee = 0.0
    
    # We iterate over the ACTUAL transactions but simulate them being processed by 'scheme'
    for _, tx in df_may.iterrows():
        # Determine transaction context
        is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
        
        ctx = {
            'card_scheme': scheme, # SIMULATED SCHEME
            'account_type': m_info['account_type'],
            'mcc': m_info['merchant_category_code'],
            'capture_delay': m_info['capture_delay'],
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_level,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': is_intracountry,
            'amount': tx['eur_amount']
        }
        
        # Find matching fee rule
        # We assume the first matching rule is the correct one (or they are mutually exclusive)
        matched_rule = None
        for rule in fees:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            # Fee = Fixed + (Rate * Amount / 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * ctx['amount'] / 10000.0)
            total_fee += fee
        else:
            # If no rule matches, we assume 0 or log it. 
            # For robustness, we'll skip adding cost (or could add a default).
            pass
            
    scheme_totals[scheme] = total_fee

# 4. Find the Maximum
max_scheme = max(scheme_totals, key=scheme_totals.get)
max_fee = scheme_totals[max_scheme]

# Output the result
print(max_scheme)

# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1730
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: insufficient
# Plan steps: 2
# Code length: 7988 characters (FULL CODE)
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
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60")
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
    return float(value)

def parse_range_string(range_str):
    """Parses strings like '100k-1m', '7.7%-8.3%', '<3', '>5' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower()
    
    # Handle k/m suffixes
    def parse_val(val_s):
        val_s = val_s.strip()
        multiplier = 1
        if val_s.endswith('k'):
            multiplier = 1000
            val_s = val_s[:-1]
        elif val_s.endswith('m'):
            multiplier = 1000000
            val_s = val_s[:-1]
        elif '%' in val_s:
            return float(val_s.replace('%', '')) / 100
        try:
            return float(val_s) * multiplier
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('>'):
        return parse_val(s[1:]), float('inf')
    elif s.startswith('<'):
        return float('-inf'), parse_val(s[1:])
    else:
        # Exact value treated as range [val, val]
        try:
            val = parse_val(s)
            return val, val
        except:
            return None, None

def check_capture_delay(rule_delay, merchant_delay):
    """Checks if merchant capture delay matches the rule."""
    if rule_delay is None:
        return True
    
    r = str(rule_delay).lower()
    m = str(merchant_delay).lower()
    
    if r == m:
        return True
    
    # Map merchant "immediate" to 0 days, "manual" to inf
    def to_num(val):
        if val == 'immediate': return 0.0
        if val == 'manual': return float('inf')
        try:
            return float(val)
        except:
            return None

    m_val = to_num(m)
    
    if r.startswith('<') and m_val is not None:
        limit = float(r[1:])
        return m_val < limit
    elif r.startswith('>') and m_val is not None:
        limit = float(r[1:])
        return m_val > limit
    elif '-' in r and m_val is not None:
        low, high = map(float, r.split('-'))
        return low <= m_val <= high
        
    return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match or wildcard)
    if rule['account_type'] and tx_context['account_type'] not in rule['account_type']:
        return False

    # 3. Merchant Category Code (List match or wildcard)
    if rule['merchant_category_code'] and tx_context['mcc'] not in rule['merchant_category_code']:
        return False

    # 4. Is Credit (Boolean match or wildcard)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_context['is_credit']:
        return False

    # 5. ACI (List match or wildcard)
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False

    # 6. Intracountry (Boolean match or wildcard)
    if rule['intracountry'] is not None:
        # rule['intracountry'] is 0.0 or 1.0 in JSON, convert to bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    # 7. Capture Delay (Complex match)
    if not check_capture_delay(rule['capture_delay'], tx_context['capture_delay']):
        return False

    # 8. Monthly Volume (Range match)
    if rule['monthly_volume']:
        min_v, max_v = parse_range_string(rule['monthly_volume'])
        if min_v is not None and not (min_v <= tx_context['monthly_volume'] <= max_v):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule['monthly_fraud_level']:
        min_f, max_f = parse_range_string(rule['monthly_fraud_level'])
        if min_f is not None and not (min_f <= tx_context['monthly_fraud_level'] <= max_f):
            return False

    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# File paths
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Get Merchant Details
target_merchant = "Martinis_Fine_Steakhouse"
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)

if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

m_account_type = merchant_info['account_type']
m_mcc = merchant_info['merchant_category_code']
m_capture_delay = merchant_info['capture_delay']

# 3. Calculate Monthly Stats (Volume & Fraud) for January 2023
# Manual: "computed always in natural months... starting always in day 1 and ending in the last natural day"
# Jan 2023 is day_of_year 1 to 31
jan_mask = (
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == 2023) & 
    (df_payments['day_of_year'] >= 1) & 
    (df_payments['day_of_year'] <= 31)
)
df_jan = df_payments[jan_mask]

# Volume: Sum of eur_amount
monthly_volume = df_jan['eur_amount'].sum()

# Fraud Level: Ratio of fraudulent volume over total volume (per Manual Section 7)
fraud_volume = df_jan[df_jan['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 4. Filter Target Transactions (Day 12)
target_mask = (
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == 2023) & 
    (df_payments['day_of_year'] == 12)
)
df_target = df_payments[target_mask].copy()

# 5. Calculate Fees
total_fees = 0.0
matched_count = 0

for _, row in df_target.iterrows():
    # Determine intracountry status
    # "True if the transaction is domestic, defined by the fact that the issuer country and the acquiring country are the same."
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
    # Build context
    tx_context = {
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': is_intracountry,
        'account_type': m_account_type,
        'mcc': m_mcc,
        'capture_delay': m_capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    # Find matching rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Fee formula: fixed + rate * amount / 10000
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * row['eur_amount'] / 10000)
        total_fees += fee
        matched_count += 1
    else:
        # Should ideally not happen if rules cover all cases
        pass

# 6. Output Result
print(f"{total_fees:.14f}")

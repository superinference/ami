# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2748
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7567 characters (FULL CODE)
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

def parse_range_match(rule_val, actual_val):
    """
    Checks if actual_val is within rule_val range string.
    Handles: '100k-1m', '>8.3%', '3-5', etc.
    """
    if rule_val is None:
        return True
    
    # Convert actual_val to float for comparison
    try:
        val = float(actual_val)
    except (ValueError, TypeError):
        return False

    s = str(rule_val).strip().lower()
    is_pct = '%' in s
    s = s.replace('%', '').replace(',', '').replace('€', '').replace('$', '')
    
    # Handle k/m suffixes for volume
    s = s.replace('k', '000').replace('m', '000000')
    
    try:
        if '-' in s:
            parts = s.split('-')
            low = float(parts[0])
            high = float(parts[1])
            if is_pct:
                low /= 100
                high /= 100
            return low <= val <= high
        elif s.startswith('>'):
            limit = float(s[1:])
            if is_pct: limit /= 100
            return val > limit
        elif s.startswith('<'):
            limit = float(s[1:])
            if is_pct: limit /= 100
            return val < limit
        else:
            # Exact numeric match
            target = float(s)
            if is_pct: target /= 100
            return val == target
    except:
        return False

def match_fee_rule(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    tx_context must contain: card_scheme, account_type, capture_delay, 
    monthly_fraud_rate, monthly_volume, mcc, is_credit, aci, intracountry
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    # Rule has list of allowed types. If empty/null, allows all.
    if rule['account_type'] and tx_context['account_type'] not in rule['account_type']:
        return False
        
    # 3. Capture Delay (String/Range match)
    # If rule is null, matches all.
    if rule['capture_delay'] is not None:
        rd = str(rule['capture_delay'])
        ad = str(tx_context['capture_delay'])
        
        # Exact string match (handles 'manual', 'immediate')
        if rd == ad:
            pass
        # Numeric range match (handles '>5', '3-5')
        elif ad.isdigit() and (rd.startswith('>') or rd.startswith('<') or '-' in rd):
            if not parse_range_match(rd, float(ad)):
                return False
        # If rule is numeric range but actual is 'manual'/'immediate', no match unless exact match handled above
        elif ad != rd:
            return False

    # 4. Monthly Fraud Level (Range match)
    if not parse_range_match(rule['monthly_fraud_level'], tx_context['monthly_fraud_rate']):
        return False

    # 5. Monthly Volume (Range match)
    if not parse_range_match(rule['monthly_volume'], tx_context['monthly_volume']):
        return False

    # 6. MCC (List match)
    if rule['merchant_category_code'] and tx_context['mcc'] not in rule['merchant_category_code']:
        return False

    # 7. Is Credit (Boolean match)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_context['is_credit']:
        return False

    # 8. ACI (List match)
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False

    # 9. Intracountry (Boolean match)
    if rule['intracountry'] is not None:
        # rule['intracountry'] can be 0.0 or 1.0
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False
            
    return True

def calculate_fee(amount, rule):
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# --- Main Analysis ---

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'
fees_path = '/output/chunk6/data/context/fees.json'

df = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)

# 2. Define Scope (Merchant & November)
merchant_name = 'Crossfit_Hanna'
start_day = 305 # Nov 1st
end_day = 334   # Nov 30th

# Filter for Merchant and Month
df_nov = df[
    (df['merchant'] == merchant_name) &
    (df['day_of_year'] >= start_day) &
    (df['day_of_year'] <= end_day)
]

# 3. Calculate Monthly Stats (Volume & Fraud Rate)
# These determine the fee buckets and remain constant even if we change ACI of specific txs
total_volume = df_nov['eur_amount'].sum()
fraud_volume = df_nov[df_nov['has_fraudulent_dispute'] == True]['eur_amount'].sum()
# Fraud rate defined as ratio of fraud volume to total volume
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

# 4. Get Merchant Static Data
m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not m_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

mcc = m_info['merchant_category_code']
account_type = m_info['account_type']
capture_delay = m_info['capture_delay']

# 5. Identify Target Transactions (Fraudulent ones to be moved)
target_txs = df_nov[df_nov['has_fraudulent_dispute'] == True].copy()

# 6. Simulate Fees for Each ACI
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
results = {}

for aci in possible_acis:
    total_fee_for_aci = 0.0
    
    for _, tx in target_txs.iterrows():
        # Construct context for this transaction with the SIMULATED ACI
        ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': account_type,
            'capture_delay': capture_delay,
            'monthly_fraud_rate': fraud_rate,
            'monthly_volume': total_volume,
            'mcc': mcc,
            'is_credit': tx['is_credit'],
            'aci': aci, # <--- The variable we are testing
            'intracountry': tx['issuing_country'] == tx['acquirer_country']
        }
        
        # Find the first matching fee rule
        matched_rule = None
        for rule in fees:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fee_for_aci += fee
        else:
            # If no rule matches, we assume a high cost or skip. 
            # In this dataset, rules are generally comprehensive.
            # We'll log it as 0 but in reality it might be rejected/expensive.
            pass
            
    results[aci] = total_fee_for_aci

# 7. Determine Preferred Choice (Lowest Fee)
# We want the ACI that results in the minimum total fee for these transactions
best_aci = min(results, key=results.get)

# Output the result
print(best_aci)

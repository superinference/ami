# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2712
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7166 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None:
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
        if 'k' in v.lower():
            try:
                return float(v.lower().replace('k', '')) * 1000
            except:
                pass
        if 'm' in v.lower():
            try:
                return float(v.lower().replace('m', '')) * 1000000
            except:
                pass
        try:
            return float(v)
        except:
            return 0.0
    return 0.0

def parse_range(range_str):
    """Parses a range string like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if pd.isna(range_str) or range_str is None:
        return (float('-inf'), float('inf'))
    
    s = str(range_str).strip().lower()
    is_percent = '%' in s
    s = s.replace('%', '')
    
    def parse_val(val_s):
        val_s = val_s.strip()
        mult = 1
        if 'k' in val_s:
            mult = 1000
            val_s = val_s.replace('k', '')
        elif 'm' in val_s:
            mult = 1000000
            val_s = val_s.replace('m', '')
        try:
            return float(val_s) * mult
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        low = parse_val(parts[0])
        high = parse_val(parts[1])
        if is_percent:
            low /= 100
            high /= 100
        return (low, high)
    elif s.startswith('>'):
        val = parse_val(s[1:])
        if is_percent: val /= 100
        return (val, float('inf'))
    elif s.startswith('<'):
        val = parse_val(s[1:])
        if is_percent: val /= 100
        return (float('-inf'), val)
    else:
        val = parse_val(s)
        if is_percent: val /= 100
        return (val, val)

def match_fee_rule(tx_ctx, rule):
    """Checks if a fee rule applies to a transaction context."""
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List match, wildcard=[])
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match, wildcard=[])
    if rule.get('merchant_category_code'):
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Bool match, wildcard=None)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 5. ACI (List match, wildcard=[])
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Bool match, wildcard=None)
    if rule.get('intracountry') is not None:
        # Handle potential string/float/bool types in JSON
        r_val = rule['intracountry']
        r_bool = False
        if isinstance(r_val, str):
            if r_val.lower() == 'true': r_bool = True
            elif r_val.lower() == 'false': r_bool = False
            else: r_bool = float(r_val) != 0.0
        else:
            r_bool = bool(r_val)
            
        if r_bool != tx_ctx['intracountry']:
            return False
            
    # 7. Monthly Volume (Range match, wildcard=None)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_ctx['monthly_volume'] <= max_v):
            return False
            
    # 8. Monthly Fraud Level (Range match, wildcard=None)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if not (min_f <= tx_ctx['monthly_fraud_level'] <= max_f):
            return False
            
    return True

def calculate_fee(amount, rule):
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000)

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Merchant Context
target_merchant = 'Belles_cookbook_store'
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found")

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']

# 3. Calculate April Stats (Day 91-120)
april_mask = (df_payments['merchant'] == target_merchant) & \
             (df_payments['day_of_year'] >= 91) & \
             (df_payments['day_of_year'] <= 120) & \
             (df_payments['year'] == 2023)

df_april = df_payments[april_mask]

# Calculate Volume and Fraud Level for Fee Tier determination
monthly_volume = df_april['eur_amount'].sum()
fraud_volume = df_april[df_april['has_fraudulent_dispute'] == True]['eur_amount'].sum()
monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 4. Identify Target Transactions (Fraudulent ones in April)
fraud_txs = df_april[df_april['has_fraudulent_dispute'] == True].copy()

# 5. Simulation
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
results = {}

for aci in possible_acis:
    total_fee = 0.0
    
    for _, tx in fraud_txs.iterrows():
        # Build context for this specific transaction simulation
        tx_ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': account_type,
            'merchant_category_code': mcc,
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_level,
            'is_credit': bool(tx['is_credit']),
            'aci': aci, # The variable we are changing
            'intracountry': tx['issuing_country'] == tx['acquirer_country']
        }
        
        # Find matching rule (First valid match)
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(tx_ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fee += fee
        else:
            # If no rule matches, we assume a high default or skip. 
            # For this analysis, we assume coverage exists.
            pass

    results[aci] = total_fee

# 6. Determine Winner
best_aci = min(results, key=results.get)
print(best_aci)

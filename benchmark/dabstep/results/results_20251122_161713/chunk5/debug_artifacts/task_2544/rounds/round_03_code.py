# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2544
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 7893 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v.lower():
            return float(v.lower().replace('k', '')) * 1000
        if 'm' in v.lower():
            return float(v.lower().replace('m', '')) * 1000000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value) if value is not None else 0.0

def get_month(day_of_year):
    """Returns month (1-12) from day_of_year for non-leap year 2023."""
    days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cumulative = 0
    for i, d in enumerate(days):
        cumulative += d
        if day_of_year <= cumulative:
            return i + 1
    return 12

def parse_range(range_str):
    """Parses 'min-max', '>min', '<max' string to tuple (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    if range_str.startswith('>'):
        return coerce_to_float(range_str[1:]), float('inf')
    if range_str.startswith('<'):
        return float('-inf'), coerce_to_float(range_str[1:])
        
    parts = range_str.split('-')
    if len(parts) == 2:
        return coerce_to_float(parts[0]), coerce_to_float(parts[1])
    return None, None

def check_range(value, range_str):
    """Checks if value is within range_str."""
    if range_str is None:
        return True
    min_val, max_val = parse_range(range_str)
    if min_val is None: 
        return True
    return min_val <= value <= max_val

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    tx_ctx: dict containing transaction details + monthly stats
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List match, empty=all)
    if rule.get('account_type') and tx_ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. Capture Delay (Complex Match: Exact string OR Numeric Range)
    if rule.get('capture_delay') is not None:
        r_val = rule['capture_delay']
        m_val = str(tx_ctx['capture_delay'])
        
        if r_val == m_val:
            # Exact string match (e.g. "manual" == "manual")
            pass
        elif m_val.replace('.','',1).isdigit():
            # Merchant has numeric days (e.g. "7")
            # Check if rule is a range (e.g. ">5")
            # If rule is "manual" or "immediate", it won't match a number
            if r_val in ['manual', 'immediate']:
                return False
            if not check_range(float(m_val), r_val):
                return False
        else:
            # Merchant has non-numeric (e.g. "immediate") and it didn't match exact rule
            return False
        
    # 4. Merchant Category Code (List match, empty=all)
    # Note: tx_ctx['mcc'] is the MCC we are testing (Original or New)
    if rule.get('merchant_category_code') and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 5. Is Credit (Boolean, null=all)
    if rule.get('is_credit') is not None and rule['is_credit'] != tx_ctx['is_credit']:
        return False
        
    # 6. ACI (List match, empty=all)
    if rule.get('aci') and tx_ctx['aci'] not in rule['aci']:
        return False
        
    # 7. Intracountry (Boolean, null=all)
    if rule.get('intracountry') is not None:
        is_intra = (tx_ctx['issuing_country'] == tx_ctx['acquirer_country'])
        # rule['intracountry'] might be 0.0/1.0 or boolean
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 8. Monthly Volume (Range)
    if rule.get('monthly_volume') is not None:
        if not check_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 9. Monthly Fraud Level (Range)
    if rule.get('monthly_fraud_level') is not None:
        if not check_range(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False
            
    return True

def calculate_fee(amount, rule):
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# Sort fees by ID to ensure consistent priority (lower ID first)
fees_data.sort(key=lambda x: x['ID'])

# 2. Filter for Merchant and Year
target_merchant = 'Martinis_Fine_Steakhouse'
target_year = 2023
df_txs = df_payments[(df_payments['merchant'] == target_merchant) & (df_payments['year'] == target_year)].copy()

if len(df_txs) == 0:
    print("No transactions found.")
    exit()

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print("Merchant info not found.")
    exit()

original_mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats
# Add month column
df_txs['month'] = df_txs['day_of_year'].apply(get_month)

# Group by month to get volume and fraud stats
monthly_stats = {}
for month in range(1, 13):
    month_txs = df_txs[df_txs['month'] == month]
    if len(month_txs) > 0:
        vol = month_txs['eur_amount'].sum()
        # Fraud rate is ratio: fraud_vol / total_vol
        fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum()
        fraud_rate = fraud_vol / vol if vol > 0 else 0.0
        
        monthly_stats[month] = {
            'volume': vol,
            'fraud_rate': fraud_rate
        }
    else:
        monthly_stats[month] = {'volume': 0.0, 'fraud_rate': 0.0}

# 5. Calculate Fees for Both Scenarios
total_fees_original = 0.0
total_fees_new = 0.0

for _, tx in df_txs.iterrows():
    month = tx['month']
    stats = monthly_stats.get(month, {'volume': 0.0, 'fraud_rate': 0.0})
    
    # Base Context
    ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'capture_delay': capture_delay,
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'issuing_country': tx['issuing_country'],
        'acquirer_country': tx['acquirer_country'],
        'monthly_volume': stats['volume'],
        'monthly_fraud_rate': stats['fraud_rate']
    }
    
    # --- Scenario 1: Original MCC ---
    ctx['mcc'] = original_mcc
    matched_rule_orig = None
    for rule in fees_data:
        if match_fee_rule(ctx, rule):
            matched_rule_orig = rule
            break
    
    if matched_rule_orig:
        total_fees_original += calculate_fee(tx['eur_amount'], matched_rule_orig)
        
    # --- Scenario 2: New MCC (5411) ---
    ctx['mcc'] = 5411
    matched_rule_new = None
    for rule in fees_data:
        if match_fee_rule(ctx, rule):
            matched_rule_new = rule
            break
            
    if matched_rule_new:
        total_fees_new += calculate_fee(tx['eur_amount'], matched_rule_new)

# 6. Calculate Delta
delta = total_fees_new - total_fees_original

# Print with high precision
print(f"{delta:.14f}")

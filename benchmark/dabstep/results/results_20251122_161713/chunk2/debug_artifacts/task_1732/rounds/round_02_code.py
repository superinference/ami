# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1732
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 6120 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---
def coerce_to_float(value):
    """Convert string with %, $, k, m to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v.lower():
            return float(v.lower().replace('k', '')) * 1000
        if 'm' in v.lower():
            return float(v.lower().replace('m', '')) * 1000000
        return float(v)
    return float(value)

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    # Handle inequalities
    if range_str.startswith('>'):
        return coerce_to_float(range_str[1:]), float('inf')
    if range_str.startswith('<'):
        return float('-inf'), coerce_to_float(range_str[1:])
        
    parts = range_str.split('-')
    if len(parts) == 2:
        return coerce_to_float(parts[0]), coerce_to_float(parts[1])
    return None, None

def check_capture_delay(merchant_delay, rule_delay):
    """Matches merchant capture delay to rule capture delay."""
    if rule_delay is None:
        return True
    
    md = str(merchant_delay).lower()
    rd = str(rule_delay).lower()
    
    if rd == 'immediate':
        return md == 'immediate'
    if rd == 'manual':
        return md == 'manual'
    
    try:
        days = float(md)
        if rd == '<3':
            return days < 3
        if rd == '>5':
            return days > 5
        if rd == '3-5':
            return 3 <= days <= 5
    except ValueError:
        return False
    return False

def match_fee_rule(tx_context, rule):
    """Checks if a fee rule applies to a transaction context."""
    
    # 1. Card Scheme
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (Wildcard allowed)
    if rule['account_type'] and tx_context['account_type'] not in rule['account_type']:
        return False
        
    # 3. MCC (Wildcard allowed)
    if rule['merchant_category_code'] and tx_context['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (Wildcard allowed)
    if not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
        return False
        
    # 5. Monthly Volume (Range match)
    if rule['monthly_volume']:
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_context['monthly_volume'] <= max_v):
            return False
            
    # 6. Monthly Fraud Level (Range match)
    if rule['monthly_fraud_level']:
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if not (min_f <= tx_context['monthly_fraud_rate'] <= max_f):
            return False
            
    # 7. Is Credit (Wildcard allowed)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 8. ACI (Wildcard allowed)
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False
        
    # 9. Intracountry (Wildcard allowed)
    if rule['intracountry'] is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False
            
    return True

def calculate_fee(amount, rule):
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# --- Main Execution ---

# 1. Load Data
base_path = '/output/chunk2/data/context/'
df_payments = pd.read_csv(base_path + 'payments.csv')
with open(base_path + 'fees.json', 'r') as f:
    fees_data = json.load(f)
with open(base_path + 'merchant_data.json', 'r') as f:
    merchant_data = json.load(f)

# 2. Setup Target
target_merchant = 'Martinis_Fine_Steakhouse'
target_year = 2023
target_day = 200

# 3. Get Merchant Attributes
m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not m_info:
    raise ValueError(f"Merchant {target_merchant} not found")

merchant_account_type = m_info['account_type']
merchant_mcc = m_info['merchant_category_code']
merchant_capture_delay = m_info['capture_delay']

# 4. Calculate Monthly Stats (July 2023)
# July 2023 is Day 182 to 212 (Non-leap year: 31+28+31+30+31+30 = 181 days before July)
july_start = 182
july_end = 212

july_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= july_start) &
    (df_payments['day_of_year'] <= july_end)
]

monthly_volume = july_txs['eur_amount'].sum()
fraud_volume = july_txs[july_txs['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 5. Filter Transactions for Day 200
day_200_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
]

# 6. Calculate Fees
total_fees = 0.0

for idx, tx in day_200_txs.iterrows():
    # Determine intracountry status (Issuer == Acquirer)
    is_intra = (tx['issuing_country'] == tx['acquirer_country'])
    
    context = {
        'card_scheme': tx['card_scheme'],
        'account_type': merchant_account_type,
        'mcc': merchant_mcc,
        'capture_delay': merchant_capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate,
        'is_credit': bool(tx['is_credit']),
        'aci': tx['aci'],
        'intracountry': is_intra
    }
    
    # Find first matching rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee

# Output the result formatted to 2 decimal places
print(f"{total_fees:.2f}")

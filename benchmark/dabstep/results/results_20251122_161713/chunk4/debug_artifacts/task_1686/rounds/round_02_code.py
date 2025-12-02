# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1686
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8737 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean if single value needed, 
        # but usually this function is for single values. 
        # If it's a range string passed here by mistake, we try to parse the first part or return 0
        if '-' in v:
            try:
                parts = v.split('-')
                return float(parts[0])
            except:
                return 0.0
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range(range_str, scale=1.0):
    """Parses a range string like '100k-1m' or '0%-0.8%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle inequalities
    if s.startswith('>'):
        val = parse_value(s[1:])
        return val, float('inf')
    if s.startswith('<'):
        val = parse_value(s[1:])
        return float('-inf'), val
        
    # Handle ranges
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = parse_value(parts[0])
            max_val = parse_value(parts[1])
            return min_val, max_val
            
    return None, None

def parse_value(val_str):
    """Helper to parse values with k, m, % suffixes."""
    s = val_str.strip()
    multiplier = 1.0
    if '%' in s:
        multiplier = 0.01
        s = s.replace('%', '')
    elif 'k' in s:
        multiplier = 1000.0
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000.0
        s = s.replace('m', '')
        
    try:
        return float(s) * multiplier
    except ValueError:
        return 0.0

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches the rule."""
    if rule_delay is None:
        return True
    
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    # Exact match for text values
    if r_delay in ['immediate', 'manual']:
        return m_delay == r_delay
        
    # Numeric comparison
    try:
        # Convert merchant delay to number if possible (e.g. "1" -> 1)
        if m_delay.isdigit():
            m_val = float(m_delay)
        else:
            return False # Cannot compare non-digit merchant delay with numeric rule
            
        if '-' in r_delay:
            min_d, max_d = map(float, r_delay.split('-'))
            return min_d <= m_val <= max_d
        elif r_delay.startswith('>'):
            return m_val > float(r_delay[1:])
        elif r_delay.startswith('<'):
            return m_val < float(r_delay[1:])
    except:
        return False
        
    return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_ctx must contain:
    - card_scheme, is_credit, aci, issuing_country, acquirer_country
    - merchant_account_type, merchant_mcc, merchant_capture_delay
    - monthly_volume, monthly_fraud_rate
    """
    
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    # If rule['account_type'] is empty/null, it applies to all.
    # Otherwise, merchant's account type must be in the list.
    if rule.get('account_type'):
        if tx_ctx['merchant_account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_ctx['merchant_mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Complex match)
    if rule.get('capture_delay'):
        if not check_capture_delay(tx_ctx['merchant_capture_delay'], rule['capture_delay']):
            return False
            
    # 5. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 6. ACI (List match)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 7. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        is_intra = (tx_ctx['issuing_country'] == tx_ctx['acquirer_country'])
        if rule['intracountry'] != is_intra:
            return False
            
    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if min_v is not None:
            if not (min_v <= tx_ctx['monthly_volume'] <= max_v):
                return False
                
    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if min_f is not None:
            # Fraud rate in context is ratio (0.0 to 1.0), ranges are parsed to ratio
            if not (min_f <= tx_ctx['monthly_fraud_rate'] <= max_f):
                return False
                
    return True

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
merchant_path = '/output/chunk4/data/context/merchant_data.json'
fees_path = '/output/chunk4/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

target_merchant = 'Belles_cookbook_store'
target_year = 2023
target_day = 365

# 2. Get Merchant Static Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

m_account_type = merchant_info['account_type']
m_mcc = merchant_info['merchant_category_code']
m_capture_delay = merchant_info['capture_delay']

# 3. Calculate Monthly Stats (December 2023)
# December starts on day 335 (non-leap year)
dec_start = 335
dec_end = 365

df_december = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= dec_start) &
    (df_payments['day_of_year'] <= dec_end)
]

monthly_volume = df_december['eur_amount'].sum()

# Fraud rate calculation: Volume of fraud / Total Volume
fraud_txs = df_december[df_december['has_fraudulent_dispute'] == True]
fraud_volume = fraud_txs['eur_amount'].sum()

if monthly_volume > 0:
    monthly_fraud_rate = fraud_volume / monthly_volume
else:
    monthly_fraud_rate = 0.0

print(f"Merchant: {target_merchant}")
print(f"Dec Volume: {monthly_volume:.2f}")
print(f"Dec Fraud Volume: {fraud_volume:.2f}")
print(f"Dec Fraud Rate: {monthly_fraud_rate:.6f} ({monthly_fraud_rate*100:.4f}%)")

# 4. Filter Target Transactions (Day 365)
target_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
]

print(f"Transactions on day {target_day}: {len(target_txs)}")

# 5. Find Applicable Fee IDs
applicable_fee_ids = set()

for _, tx in target_txs.iterrows():
    # Build context for this transaction
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'issuing_country': tx['issuing_country'],
        'acquirer_country': tx['acquirer_country'],
        'eur_amount': tx['eur_amount'],
        
        'merchant_account_type': m_account_type,
        'merchant_mcc': m_mcc,
        'merchant_capture_delay': m_capture_delay,
        
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Check against all rules
    for rule in fees_data:
        if match_fee_rule(tx_ctx, rule):
            applicable_fee_ids.add(rule['ID'])

# 6. Output Result
sorted_ids = sorted(list(applicable_fee_ids))
print(f"Applicable Fee IDs: {sorted_ids}")

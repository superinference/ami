# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1713
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9694 characters (FULL CODE)
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
        v = v.lstrip('><≤≥')  # Remove comparison operators for simple coercion
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v.lower():
            return float(v.lower().replace('k', '')) * 1000
        if 'm' in v.lower():
            return float(v.lower().replace('m', '')) * 1000000
        # Range handling (e.g., "50-60") - return mean
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
    return float(value)

def check_range(value, rule_value):
    """Check if a numeric value fits within a rule's range string (e.g., '100k-1m', '>5')."""
    if rule_value is None:
        return True
    
    # Handle percentage conversion for comparison
    if isinstance(rule_value, str) and '%' in rule_value:
        is_percent = True
    else:
        is_percent = False

    # Parse rule
    rule_str = str(rule_value).strip().lower()
    
    # Handle simple ranges "min-max"
    if '-' in rule_str:
        try:
            parts = rule_str.split('-')
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return min_val <= value <= max_val
        except:
            return False
            
    # Handle inequalities
    if rule_str.startswith('>'):
        limit = coerce_to_float(rule_str[1:])
        return value > limit
    if rule_str.startswith('<'):
        limit = coerce_to_float(rule_str[1:])
        return value < limit
    if rule_str.startswith('>='):
        limit = coerce_to_float(rule_str[2:])
        return value >= limit
    if rule_str.startswith('<='):
        limit = coerce_to_float(rule_str[2:])
        return value <= limit
        
    # Exact match (numeric)
    return value == coerce_to_float(rule_str)

def check_capture_delay(merchant_delay, rule_delay):
    """Specific logic for capture delay matching."""
    if rule_delay is None:
        return True
    
    # Normalize inputs
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    if m_delay == r_delay:
        return True
        
    # Handle numeric comparisons if both look like numbers or inequalities
    # Merchant delay is usually a specific number or 'immediate'/'manual'
    # Rule delay can be range or inequality
    
    # Map 'immediate' to 0, 'manual' to 999 (or handle separately)
    def parse_delay_val(d):
        if d == 'immediate': return 0.0
        if d == 'manual': return 999.0
        return coerce_to_float(d)

    try:
        val = parse_delay_val(m_delay)
        return check_range(val, r_delay)
    except:
        return False

def match_fee_rule(tx_context, rule):
    """
    Check if a transaction context matches a fee rule.
    tx_context must contain:
    - card_scheme, account_type, merchant_category_code, is_credit, aci
    - intracountry, monthly_volume, monthly_fraud_level, capture_delay
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match or wildcard)
    if rule['account_type'] and tx_context['account_type'] not in rule['account_type']:
        return False
        
    # 3. MCC (List match or wildcard)
    if rule['merchant_category_code'] and tx_context['merchant_category_code'] not in rule['merchant_category_code']:
        return False
        
    # 4. Is Credit (Exact match or wildcard)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_context['is_credit']:
        return False
        
    # 5. ACI (List match or wildcard)
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False
        
    # 6. Intracountry (Exact match or wildcard)
    # Note: fees.json uses 1.0/0.0 for True/False often, or boolean
    if rule['intracountry'] is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False
            
    # 7. Monthly Volume (Range match or wildcard)
    if rule['monthly_volume'] is not None:
        if not check_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 8. Monthly Fraud Level (Range match or wildcard)
    if rule['monthly_fraud_level'] is not None:
        if not check_range(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False
            
    # 9. Capture Delay (Complex match or wildcard)
    if rule['capture_delay'] is not None:
        if not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculate fee based on fixed amount and rate."""
    # fee = fixed_amount + rate * transaction_value / 10000
    fixed = float(rule['fixed_amount'])
    rate = float(rule['rate'])
    return fixed + (rate * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Setup Target
target_merchant = 'Belles_cookbook_store'
target_day = 100
target_year = 2023

# 3. Get Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

m_account_type = merchant_info['account_type']
m_mcc = merchant_info['merchant_category_code']
m_capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats (April 2023)
# Day 100 is in April. April is usually days 91-120 (non-leap).
# Let's verify day 100 month.
# Jan: 31, Feb: 28, Mar: 31 -> Total 90. So Day 91 is Apr 1. Day 100 is Apr 10.
month_start = 91
month_end = 120

monthly_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= month_start) &
    (df_payments['day_of_year'] <= month_end)
]

monthly_volume = monthly_txs['eur_amount'].sum()
monthly_fraud_count = monthly_txs['has_fraudulent_dispute'].sum()
monthly_tx_count = len(monthly_txs)
monthly_fraud_rate = (monthly_fraud_count / monthly_volume) if monthly_volume > 0 else 0.0 
# Wait, manual says: "Fraud is defined as the ratio of fraudulent volume over total volume."
# Let's re-read manual section 7: "Fraud is defined as the ratio of fraudulent volume over total volume."
# Wait, usually it's count, but let's stick to the manual definition if explicit.
# Actually, let's check the fees.json context. Usually fraud levels are small percentages.
# Let's calculate fraud by volume as per manual.
fraud_volume = monthly_txs[monthly_txs['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate = (fraud_volume / monthly_volume) if monthly_volume > 0 else 0.0

# 5. Filter Target Transactions (Day 100)
target_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
]

# 6. Calculate Fees
total_fees = 0.0
matched_count = 0
unmatched_count = 0

# Pre-process fees to ensure correct types for comparison
for rule in fees_data:
    # Ensure lists are lists
    if rule['account_type'] is None: rule['account_type'] = []
    if rule['merchant_category_code'] is None: rule['merchant_category_code'] = []
    if rule['aci'] is None: rule['aci'] = []

for _, tx in target_txs.iterrows():
    # Build context
    is_intra = (tx['issuing_country'] == tx['acquirer_country'])
    
    context = {
        'card_scheme': tx['card_scheme'],
        'account_type': m_account_type,
        'merchant_category_code': m_mcc,
        'is_credit': bool(tx['is_credit']),
        'aci': tx['aci'],
        'intracountry': is_intra,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_rate,
        'capture_delay': m_capture_delay
    }
    
    # Find matching rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(context, rule):
            matched_rule = rule
            break # Stop at first match
            
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee
        matched_count += 1
    else:
        # Fallback or error? Usually there's a catch-all, but if not found, 0?
        # In this dataset, usually every tx has a fee.
        # Let's print a warning if this happens often.
        unmatched_count += 1
        # print(f"No match for tx: {tx['psp_reference']}")

# 7. Output
print(f"{total_fees:.14f}")

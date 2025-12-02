# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1723
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7670 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m suffixes to float."""
    if pd.isna(value) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    
    s = str(value).strip().lower().replace(',', '').replace('€', '').replace('$', '')
    s = s.lstrip('><≤≥')
    
    try:
        if '%' in s:
            return float(s.replace('%', '')) / 100.0
        if s.endswith('k'):
            return float(s[:-1]) * 1000
        if s.endswith('m'):
            return float(s[:-1]) * 1000000
        return float(s)
    except ValueError:
        return 0.0

def parse_range(rule_val, actual_val):
    """
    Checks if actual_val falls within the range specified by rule_val.
    rule_val examples: '100k-1m', '>5', '<3', '7.7%-8.3%', 'immediate'
    """
    if rule_val is None:
        return True
    
    # Handle string equality (e.g. 'immediate', 'manual')
    if isinstance(rule_val, str) and '-' not in rule_val and '>' not in rule_val and '<' not in rule_val and '%' not in rule_val and not rule_val[0].isdigit():
         return str(rule_val).lower() == str(actual_val).lower()

    # Handle numeric comparisons
    try:
        val = float(actual_val)
    except (ValueError, TypeError):
        # If actual value isn't a number but rule implies number, return False
        return False

    s = str(rule_val).strip().lower()
    
    if s.startswith('>'):
        limit = coerce_to_float(s[1:])
        return val > limit
    if s.startswith('<'):
        limit = coerce_to_float(s[1:])
        return val < limit
    
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return min_val <= val <= max_val
            
    # Direct equality for numbers
    return val == coerce_to_float(s)

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_ctx: dictionary containing transaction and merchant details
    rule: dictionary from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False

    # 2. Account Type (List match)
    # Rule has list of allowed types. Merchant has one type.
    if rule.get('account_type'):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_ctx.get('merchant_category_code') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_ctx.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        if rule['intracountry'] != tx_ctx.get('intracountry'):
            return False

    # 7. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range(rule['monthly_volume'], tx_ctx.get('monthly_volume')):
            return False

    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not parse_range(rule['monthly_fraud_level'], tx_ctx.get('monthly_fraud_level')):
            return False
            
    # 9. Capture Delay (Exact/Range match) - usually string in merchant data
    if rule.get('capture_delay'):
        # Merchant data has capture_delay, check if it matches rule
        if not parse_range(rule['capture_delay'], tx_ctx.get('capture_delay')):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Define Target
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023
target_day = 10

# 3. Get Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

mcc = merchant_info.get('merchant_category_code')
account_type = merchant_info.get('account_type')
capture_delay = merchant_info.get('capture_delay')

# 4. Calculate Monthly Stats (January 2023)
# Filter for Jan 2023 (Day 1-31)
jan_mask = (
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year) & 
    (df_payments['day_of_year'] >= 1) & 
    (df_payments['day_of_year'] <= 31)
)
df_jan = df_payments[jan_mask]

monthly_volume = df_jan['eur_amount'].sum()

# Fraud Level: Ratio of Fraud Volume / Total Volume
# Note: Manual says "ratio between monthly total volume and monthly volume notified as fraud"
# Standard interpretation: Fraud Volume / Total Volume
fraud_txs = df_jan[df_jan['has_fraudulent_dispute'] == True]
fraud_volume = fraud_txs['eur_amount'].sum()

if monthly_volume > 0:
    monthly_fraud_level = fraud_volume / monthly_volume
else:
    monthly_fraud_level = 0.0

# 5. Filter Target Transactions (Day 10)
day_mask = (
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year) & 
    (df_payments['day_of_year'] == target_day)
)
df_target = df_payments[day_mask]

# 6. Calculate Fees
total_fees = 0.0
transaction_count = len(df_target)

# Pre-calculate context parts that don't change per transaction
base_ctx = {
    'merchant_category_code': mcc,
    'account_type': account_type,
    'capture_delay': capture_delay,
    'monthly_volume': monthly_volume,
    'monthly_fraud_level': monthly_fraud_level
}

for _, tx in df_target.iterrows():
    # Build transaction context
    tx_ctx = base_ctx.copy()
    tx_ctx['card_scheme'] = tx['card_scheme']
    tx_ctx['is_credit'] = tx['is_credit']
    tx_ctx['aci'] = tx['aci']
    tx_ctx['eur_amount'] = tx['eur_amount']
    
    # Determine intracountry
    # Intracountry is True if issuing_country == acquirer_country
    # Note: payments.csv has 'acquirer_country'
    is_intra = (tx['issuing_country'] == tx['acquirer_country'])
    tx_ctx['intracountry'] = is_intra
    
    # Find matching rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(tx_ctx, rule):
            matched_rule = rule
            break # Use first matching rule
    
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee
    else:
        # Fallback or error? Assuming data is complete, this shouldn't happen often.
        # If no rule matches, fee is 0 (or could be a default, but not specified).
        pass

# 7. Output Result
print(f"{total_fees:.2f}")

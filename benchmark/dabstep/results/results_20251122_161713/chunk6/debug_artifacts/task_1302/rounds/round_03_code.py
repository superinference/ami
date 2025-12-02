# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1302
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: insufficient
# Plan steps: 2
# Code length: 8513 characters (FULL CODE)
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

def parse_volume_range(range_str, actual_value):
    """Check if actual_value falls within the volume range string (e.g., '100k-1m')."""
    if range_str is None:
        return True
    
    def parse_val(s):
        s = s.lower().strip()
        mult = 1
        if 'k' in s:
            mult = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mult = 1000000
            s = s.replace('m', '')
        return float(s) * mult

    try:
        if '-' in range_str:
            low, high = map(parse_val, range_str.split('-'))
            return low <= actual_value <= high
        elif '>' in range_str:
            val = parse_val(range_str.replace('>', ''))
            return actual_value > val
        elif '<' in range_str:
            val = parse_val(range_str.replace('<', ''))
            return actual_value < val
    except:
        return False
    return False

def parse_fraud_range(range_str, actual_rate):
    """Check if actual_rate falls within the fraud range string (e.g., '0.0%-0.8%')."""
    if range_str is None:
        return True
    
    try:
        if '-' in range_str:
            parts = range_str.split('-')
            low = coerce_to_float(parts[0])
            high = coerce_to_float(parts[1])
            return low <= actual_rate <= high
        elif '>' in range_str:
            val = coerce_to_float(range_str)
            return actual_rate > val
        elif '<' in range_str:
            val = coerce_to_float(range_str)
            return actual_rate < val
    except:
        return False
    return False

def check_capture_delay(rule_delay, actual_delay):
    """Check if actual_delay matches the rule_delay logic."""
    if rule_delay is None:
        return True
    
    # Direct match (handles 'immediate', 'manual')
    if str(rule_delay) == str(actual_delay):
        return True
        
    # Numeric comparison if possible
    try:
        act_val = int(actual_delay)
        
        if '-' in rule_delay: # e.g. "3-5"
            low, high = map(int, rule_delay.split('-'))
            return low <= act_val <= high
        elif '>' in rule_delay: # e.g. ">5"
            val = int(rule_delay.replace('>', ''))
            return act_val > val
        elif '<' in rule_delay: # e.g. "<3"
            val = int(rule_delay.replace('<', ''))
            return act_val < val
    except ValueError:
        pass
        
    return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_ctx: dict containing transaction details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Credit/Debit
    # rule['is_credit'] can be True, False, or None (wildcard)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_ctx['is_credit']:
        return False
        
    # 3. Merchant Category Code (MCC)
    # rule['merchant_category_code'] is a list or empty (wildcard)
    if rule['merchant_category_code'] and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Account Type
    # rule['account_type'] is a list or empty (wildcard)
    if rule['account_type'] and tx_ctx['account_type'] not in rule['account_type']:
        return False
        
    # 5. ACI
    # rule['aci'] is a list or empty (wildcard)
    if rule['aci'] and tx_ctx['aci'] not in rule['aci']:
        return False
        
    # 6. Intracountry
    # rule['intracountry'] is bool or None (wildcard)
    if rule['intracountry'] is not None:
        # tx_ctx['intracountry'] is bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False

    # 7. Capture Delay
    if not check_capture_delay(rule['capture_delay'], tx_ctx['capture_delay']):
        return False

    # 8. Monthly Volume
    if not parse_volume_range(rule['monthly_volume'], tx_ctx['monthly_volume']):
        return False
        
    # 9. Monthly Fraud Level
    if not parse_fraud_range(rule['monthly_fraud_level'], tx_ctx['monthly_fraud_rate']):
        return False
        
    return True

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments = pd.read_csv('/output/chunk6/data/context/payments.csv')
fees = pd.read_json('/output/chunk6/data/context/fees.json')
merchant_data = pd.read_json('/output/chunk6/data/context/merchant_data.json')

# 2. Preprocessing: Add Month to Payments
# Convert year/day_of_year to a datetime to extract month
payments['date'] = pd.to_datetime(payments['year'] * 1000 + payments['day_of_year'], format='%Y%j')
payments['month'] = payments['date'].dt.month

# 3. Calculate Merchant Monthly Stats
# Volume: Sum of eur_amount
# Fraud: Sum of eur_amount where has_fraudulent_dispute is True (Fraud Volume, not count)
monthly_stats = payments.groupby(['merchant', 'month']).agg(
    total_volume=('eur_amount', 'sum'),
    fraud_volume=('eur_amount', lambda x: x[payments.loc[x.index, 'has_fraudulent_dispute']].sum())
).reset_index()

monthly_stats['fraud_rate'] = monthly_stats['fraud_volume'] / monthly_stats['total_volume']
monthly_stats['fraud_rate'] = monthly_stats['fraud_rate'].fillna(0.0)

# 4. Filter Target Transactions
# "For credit transactions... NexPay"
target_txs = payments[
    (payments['card_scheme'] == 'NexPay') & 
    (payments['is_credit'] == True)
].copy()

# 5. Enrich Transactions
# Merge merchant static data
target_txs = target_txs.merge(merchant_data, on='merchant', how='left')

# Merge monthly stats
target_txs = target_txs.merge(monthly_stats, on=['merchant', 'month'], how='left')

# Calculate Intracountry (Issuer == Acquirer)
target_txs['intracountry'] = target_txs['issuing_country'] == target_txs['acquirer_country']

# 6. Calculate Fees
# Convert fees dataframe to list of dicts for easier iteration
fee_rules = fees.to_dict('records')

calculated_fees = []
target_amount = 4321.0

# Optimization: Pre-filter fee rules for NexPay and Credit=True (or None)
nexpay_rules = [
    r for r in fee_rules 
    if r['card_scheme'] == 'NexPay' 
    and (r['is_credit'] is None or r['is_credit'] is True)
]

# Iterate through each transaction to find the applicable fee
for _, tx in target_txs.iterrows():
    # Build context
    ctx = {
        'card_scheme': 'NexPay',
        'is_credit': True,
        'mcc': tx['merchant_category_code'],
        'account_type': tx['account_type'],
        'aci': tx['aci'],
        'intracountry': tx['intracountry'],
        'capture_delay': tx['capture_delay'],
        'monthly_volume': tx['total_volume'],
        'monthly_fraud_rate': tx['fraud_rate']
    }
    
    matched_rule = None
    # Find first matching rule
    for rule in nexpay_rules:
        if match_fee_rule(ctx, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Fee = Fixed + (Rate * Amount / 10000)
        # Rate is integer, e.g., 19 means 0.0019 multiplier
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * target_amount / 10000)
        calculated_fees.append(fee)
    else:
        # Fallback or error logging if no rule matches
        pass

# 7. Compute Average
if calculated_fees:
    average_fee = sum(calculated_fees) / len(calculated_fees)
    print(f"{average_fee:.14f}")
else:
    print("No applicable fees found.")

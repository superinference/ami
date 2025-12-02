# ═══════════════════════════════════════════════════════════
# Round 6 - Task 1302
# ═══════════════════════════════════════════════════════════
# Temperature: 0.50
# Verification: insufficient
# Plan steps: 3
# Code length: 8516 characters (FULL CODE)
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
        v = v.lstrip('><≤≥')
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except:
                return 0.0
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
        try:
            return float(s) * mult
        except:
            return 0.0

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
        # Convert actual_rate (ratio) to percentage for comparison if needed, 
        # but coerce_to_float handles '8.3%' -> 0.083. 
        # Let's assume actual_rate is a ratio (0.083).
        
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
    if str(rule_delay).lower() == str(actual_delay).lower():
        return True
        
    # Numeric comparison if possible
    try:
        act_val = float(actual_delay)
        if '-' in rule_delay: # e.g. "3-5"
            low, high = map(float, rule_delay.split('-'))
            return low <= act_val <= high
        elif '>' in rule_delay: # e.g. ">5"
            val = float(rule_delay.replace('>', ''))
            return act_val > val
        elif '<' in rule_delay: # e.g. "<3"
            val = float(rule_delay.replace('<', ''))
            return act_val < val
    except ValueError:
        pass
    return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Credit/Debit (rule['is_credit'] can be None/Wildcard)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
        
    # 3. Merchant Category Code (MCC)
    # Rule has list of ints, tx has int
    if rule['merchant_category_code']: # If list is not empty
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
        
    # 4. Account Type
    # Rule has list of strings, tx has string
    if rule['account_type']: # If list is not empty
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
        
    # 5. ACI
    # Rule has list of strings, tx has string
    if rule['aci']: # If list is not empty
        if tx_ctx['aci'] not in rule['aci']:
            return False
        
    # 6. Intracountry
    if rule['intracountry'] is not None:
        # Convert rule value to bool (0.0 -> False, 1.0 -> True)
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

# 3. Calculate Merchant Monthly Stats (Volume & Fraud)
# Note: Calculated on ALL transactions for the merchant, as per manual ("monthly total volume")
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
# Merge merchant static data (MCC, Account Type, Capture Delay)
target_txs = target_txs.merge(merchant_data, on='merchant', how='left')

# Merge monthly stats (Volume, Fraud Rate)
target_txs = target_txs.merge(monthly_stats, on=['merchant', 'month'], how='left')

# Calculate Intracountry (Issuer == Acquirer)
target_txs['intracountry'] = target_txs['issuing_country'] == target_txs['acquirer_country']

# 6. Calculate Fees
fee_rules = fees.to_dict('records')

# Optimization: Pre-filter fee rules for NexPay and Credit=True (or None)
nexpay_rules = [
    r for r in fee_rules 
    if r['card_scheme'] == 'NexPay' 
    and (r['is_credit'] is None or r['is_credit'] is True)
]

calculated_fees = []
target_amount = 4321.0

# Debugging counters
match_count = 0
no_match_count = 0

for _, tx in target_txs.iterrows():
    # Build context for matching
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
        match_count += 1
        # Fee = Fixed + (Rate * Amount / 10000)
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * target_amount / 10000)
        calculated_fees.append(fee)
    else:
        no_match_count += 1

# 7. Compute Average
if calculated_fees:
    average_fee = sum(calculated_fees) / len(calculated_fees)
    # Print with high precision
    print(f"{average_fee:.14f}")
else:
    # Fallback output if no fees found (should not happen if logic is correct)
    print("No applicable fees found.")

# ═══════════════════════════════════════════════════════════
# Round 7 - Task 1302
# ═══════════════════════════════════════════════════════════
# Temperature: 0.55
# Verification: insufficient
# Plan steps: 4
# Code length: 9336 characters (FULL CODE)
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
        # actual_rate is a float (e.g. 0.083). range_str might be '8.3%'.
        # coerce_to_float handles '8.3%' -> 0.083.
        
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
        # Ensure boolean comparison
        if bool(rule['is_credit']) != bool(tx_ctx['is_credit']):
            return False
        
    # 3. Merchant Category Code (MCC)
    if rule['merchant_category_code']: # If list is not empty
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
        
    # 4. Account Type
    if rule['account_type']: # If list is not empty
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
        
    # 5. ACI
    if rule['aci']: # If list is not empty
        if tx_ctx['aci'] not in rule['aci']:
            return False
        
    # 6. Intracountry
    if rule['intracountry'] is not None:
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
try:
    fees = pd.read_json('/output/chunk6/data/context/fees.json')
    payments = pd.read_csv('/output/chunk6/data/context/payments.csv')
    merchant_data = pd.read_json('/output/chunk6/data/context/merchant_data.json')
except Exception as e:
    print(f"Error loading data: {e}")
    exit()

# ═══════════════════════════════════════════════════════════
# STEP 1: INSPECT FEES (Implementing the Current Plan)
# ═══════════════════════════════════════════════════════════
print("--- NexPay Fee Rules Inspection ---")
nexpay_fees = fees[fees['card_scheme'] == 'NexPay'].copy()

# Display requested columns + context columns
cols_to_show = ['ID', 'is_credit', 'fixed_amount', 'rate', 'monthly_volume', 'monthly_fraud_level', 'aci', 'intracountry']
# Ensure columns exist
cols_to_show = [c for c in cols_to_show if c in nexpay_fees.columns]

if not nexpay_fees.empty:
    print(nexpay_fees[cols_to_show].to_string(index=False))
    print("\nUnique 'is_credit' values in NexPay rules:", nexpay_fees['is_credit'].unique())
else:
    print("No NexPay rules found in fees.json")
print("-----------------------------------\n")

# ═══════════════════════════════════════════════════════════
# STEP 2: CALCULATE AVERAGE FEE (Answering the Overall Question)
# ═══════════════════════════════════════════════════════════

# 2.1 Preprocessing: Add Month to Payments
payments['date'] = pd.to_datetime(payments['year'] * 1000 + payments['day_of_year'], format='%Y%j')
payments['month'] = payments['date'].dt.month

# 2.2 Calculate Merchant Monthly Stats (Volume & Fraud)
# Group by Year/Month to be precise, though year is 2023
monthly_stats = payments.groupby(['merchant', 'year', 'month']).agg(
    total_volume=('eur_amount', 'sum'),
    fraud_volume=('eur_amount', lambda x: x[payments.loc[x.index, 'has_fraudulent_dispute']].sum())
).reset_index()

monthly_stats['fraud_rate'] = monthly_stats['fraud_volume'] / monthly_stats['total_volume']
monthly_stats['fraud_rate'] = monthly_stats['fraud_rate'].fillna(0.0)

# 2.3 Filter Target Transactions
# "For credit transactions... NexPay"
target_txs = payments[
    (payments['card_scheme'] == 'NexPay') & 
    (payments['is_credit'] == True)
].copy()

if target_txs.empty:
    print("No NexPay credit transactions found in payments.csv.")
else:
    # 2.4 Enrich Transactions
    # Merge merchant static data
    target_txs = target_txs.merge(merchant_data, on='merchant', how='left')
    # Merge monthly stats
    target_txs = target_txs.merge(monthly_stats, on=['merchant', 'year', 'month'], how='left')
    # Calculate Intracountry (Issuer == Acquirer)
    target_txs['intracountry'] = target_txs['issuing_country'] == target_txs['acquirer_country']

    # 2.5 Calculate Fees
    fee_rules = fees.to_dict('records')
    # Pre-filter for efficiency
    nexpay_rules = [r for r in fee_rules if r['card_scheme'] == 'NexPay']

    calculated_fees = []
    target_amount = 4321.0
    
    for _, tx in target_txs.iterrows():
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
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * target_amount / 10000)
            calculated_fees.append(fee)

    # 2.6 Output Result
    print(f"Total Target Transactions: {len(target_txs)}")
    print(f"Matched Transactions: {len(calculated_fees)}")
    
    if calculated_fees:
        average_fee = sum(calculated_fees) / len(calculated_fees)
        # Print with high precision as requested by patterns
        print(f"{average_fee:.14f}")
    else:
        print("No applicable fees found.")

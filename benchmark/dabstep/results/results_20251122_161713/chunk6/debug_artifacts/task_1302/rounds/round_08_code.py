# ═══════════════════════════════════════════════════════════
# Round 8 - Task 1302
# ═══════════════════════════════════════════════════════════
# Temperature: 0.60
# Verification: sufficient
# Plan steps: 5
# Code length: 9206 characters (FULL CODE)
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
        s = str(s).lower().strip()
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
            low_str, high_str = range_str.split('-')
            low = parse_val(low_str)
            high = parse_val(high_str)
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
        # actual_rate is a float (e.g. 0.083 for 8.3%)
        # range_str might be '7.7%-8.3%'
        
        if '-' in range_str:
            parts = range_str.split('-')
            low = coerce_to_float(parts[0])
            high = coerce_to_float(parts[1])
            # Handle precision issues slightly if needed, but float comparison usually ok here
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
    
    rd = str(rule_delay).lower().strip()
    ad = str(actual_delay).lower().strip()

    # Direct match (handles 'immediate', 'manual')
    if rd == ad:
        return True
        
    # Numeric comparison if possible
    try:
        # If actual delay is a number (string representation)
        act_val = float(ad)
        
        if '-' in rd: # e.g. "3-5"
            low, high = map(float, rd.split('-'))
            return low <= act_val <= high
        elif '>' in rd: # e.g. ">5"
            val = float(rd.replace('>', ''))
            return act_val > val
        elif '<' in rd: # e.g. "<3"
            val = float(rd.replace('<', ''))
            return act_val < val
    except ValueError:
        # If actual delay is not numeric (e.g. "immediate") but rule is numeric, it's a mismatch
        # unless handled by direct match above
        pass
    return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    Returns True if match, False otherwise.
    """
    # 1. Card Scheme (Already filtered, but good for safety)
    if rule.get('card_scheme') != tx_ctx['card_scheme']:
        return False
        
    # 2. Credit/Debit
    # Rule: True, False, or None (Wildcard)
    # Ctx: True (since we filtered for credit)
    if rule['is_credit'] is not None:
        if bool(rule['is_credit']) != tx_ctx['is_credit']:
            return False
        
    # 3. Merchant Category Code (MCC)
    # Rule: List of ints or Empty (Wildcard)
    if rule['merchant_category_code']: # If list is not empty
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
        
    # 4. Account Type
    # Rule: List of strings or Empty (Wildcard)
    if rule['account_type']: # If list is not empty
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
        
    # 5. ACI
    # Rule: List of strings or Empty (Wildcard)
    if rule['aci']: # If list is not empty
        if tx_ctx['aci'] not in rule['aci']:
            return False
        
    # 6. Intracountry
    # Rule: True, False, or None (Wildcard)
    if rule['intracountry'] is not None:
        # Ensure boolean comparison
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
    fees_df = pd.read_json('/output/chunk6/data/context/fees.json')
    payments = pd.read_csv('/output/chunk6/data/context/payments.csv')
    merchant_data = pd.read_json('/output/chunk6/data/context/merchant_data.json')
except Exception as e:
    print(f"Error loading data: {e}")
    exit()

# 2. Preprocessing

# 2.1 Convert day_of_year to month to aggregate monthly stats
# Assuming year 2023 as per data inspection
payments['date'] = pd.to_datetime(payments['year'] * 1000 + payments['day_of_year'], format='%Y%j')
payments['month'] = payments['date'].dt.month

# 2.2 Calculate Merchant Monthly Stats
# Volume: Sum of eur_amount
# Fraud Rate: Sum of fraud eur_amount / Total Volume
monthly_stats = payments.groupby(['merchant', 'month']).agg(
    total_volume=('eur_amount', 'sum'),
    fraud_volume=('eur_amount', lambda x: x[payments.loc[x.index, 'has_fraudulent_dispute']].sum())
).reset_index()

monthly_stats['fraud_rate'] = monthly_stats['fraud_volume'] / monthly_stats['total_volume']
monthly_stats['fraud_rate'] = monthly_stats['fraud_rate'].fillna(0.0)

# 2.3 Filter for Target Transactions
# Question: "For credit transactions... NexPay"
target_txs = payments[
    (payments['card_scheme'] == 'NexPay') & 
    (payments['is_credit'] == True)
].copy()

if target_txs.empty:
    print("No NexPay credit transactions found.")
    exit()

# 2.4 Enrich Target Transactions
# Join Merchant Data (MCC, Account Type, Capture Delay)
target_txs = target_txs.merge(merchant_data, on='merchant', how='left')

# Join Monthly Stats (Volume, Fraud Rate) based on merchant and month
target_txs = target_txs.merge(monthly_stats, on=['merchant', 'month'], how='left')

# Calculate Intracountry (Issuer == Acquirer)
target_txs['intracountry'] = target_txs['issuing_country'] == target_txs['acquirer_country']

# 3. Fee Calculation

# Convert fees dataframe to list of dicts for easier iteration
# Filter for NexPay rules only to speed up
nexpay_rules = fees_df[fees_df['card_scheme'] == 'NexPay'].to_dict('records')

calculated_fees = []
target_amount = 4321.0

# Iterate through each transaction and find the matching fee
for idx, tx in target_txs.iterrows():
    # Create Context
    ctx = {
        'card_scheme': 'NexPay',
        'is_credit': True, # We filtered for this
        'mcc': tx['merchant_category_code'],
        'account_type': tx['account_type'],
        'aci': tx['aci'],
        'intracountry': tx['intracountry'],
        'capture_delay': tx['capture_delay'],
        'monthly_volume': tx['total_volume'],
        'monthly_fraud_rate': tx['fraud_rate']
    }
    
    matched_rule = None
    # Find the first matching rule
    # Assuming rules are prioritized by order in file or first match is sufficient
    for rule in nexpay_rules:
        if match_fee_rule(ctx, rule):
            matched_rule = rule
            break
    
    if matched_rule:
        # Calculate Fee
        # Fee = Fixed + (Rate * Amount / 10000)
        # Ensure rate is treated as numeric
        rate = coerce_to_float(matched_rule['rate'])
        fixed = coerce_to_float(matched_rule['fixed_amount'])
        
        fee = fixed + (rate * target_amount / 10000.0)
        calculated_fees.append(fee)

# 4. Final Result
if calculated_fees:
    average_fee = sum(calculated_fees) / len(calculated_fees)
    print(f"{average_fee:.14f}")
else:
    print("No applicable fees found for the filtered transactions.")

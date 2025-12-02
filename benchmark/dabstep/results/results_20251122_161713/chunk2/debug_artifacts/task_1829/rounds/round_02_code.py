# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1829
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7984 characters (FULL CODE)
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

def parse_volume_string(vol_str):
    """Parses volume strings like '100k-1m', '>10m' into (min, max)."""
    if not vol_str:
        return -float('inf'), float('inf')
    
    s = vol_str.lower().replace('€', '').replace(',', '')
    
    def parse_val(x):
        mult = 1
        if 'k' in x:
            mult = 1000
            x = x.replace('k', '')
        elif 'm' in x:
            mult = 1000000
            x = x.replace('m', '')
        return float(x) * mult

    if '-' in s:
        low, high = s.split('-')
        return parse_val(low), parse_val(high)
    elif '>' in s:
        val = parse_val(s.replace('>', ''))
        return val, float('inf')
    elif '<' in s:
        val = parse_val(s.replace('<', ''))
        return -float('inf'), val
    return -float('inf'), float('inf')

def parse_fraud_string(fraud_str):
    """Parses fraud strings like '0%-1%', '>8.3%' into (min, max)."""
    if not fraud_str:
        return -float('inf'), float('inf')
    
    s = fraud_str.replace('%', '')
    
    if '-' in s:
        low, high = s.split('-')
        return float(low)/100, float(high)/100
    elif '>' in s:
        val = float(s.replace('>', ''))
        return val/100, float('inf')
    elif '<' in s:
        val = float(s.replace('<', ''))
        return -float('inf'), val/100
    return -float('inf'), float('inf')

def match_fee_rule(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    tx_context must contain:
      - card_scheme (str)
      - account_type (str)
      - capture_delay (str)
      - mcc (int)
      - is_credit (bool)
      - aci (str)
      - intracountry (bool)
      - monthly_volume (float)
      - monthly_fraud_rate (float)
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match)
    # Rule has list of types (e.g., ['F', 'H']). Merchant has single type (e.g., 'F').
    # Empty list in rule means wildcard.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Capture Delay (String match)
    # Rule has specific value or null.
    if rule.get('capture_delay') is not None:
        if str(rule['capture_delay']) != str(tx_context['capture_delay']):
            return False

    # 4. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 5. Is Credit (Bool match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 6. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Bool/Float match)
    # Rule might have 0.0 (False), 1.0 (True), or None
    if rule.get('intracountry') is not None:
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_context['intracountry']:
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_vol, max_vol = parse_volume_string(rule['monthly_volume'])
        if not (min_vol <= tx_context['monthly_volume'] <= max_vol):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_fraud, max_fraud = parse_fraud_string(rule['monthly_fraud_level'])
        # Use a small epsilon for float comparison if needed, but direct comparison usually ok here
        if not (min_fraud <= tx_context['monthly_fraud_rate'] <= max_fraud):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    # Formula: fixed + (rate * amount / 10000)
    return fixed + (rate * amount / 10000)

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data_list = json.load(f)

# 2. Define Target Context
target_merchant = 'Crossfit_Hanna'
target_year = 2023
# July 2023: Days 182 to 212
start_day = 182
end_day = 212

# 3. Filter Payments for Target Merchant and Month
# We need the specific month's data to calculate volume and fraud stats for that month
df_july = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= start_day) &
    (df_payments['day_of_year'] <= end_day)
].copy()

if df_july.empty:
    print(0.0)
    exit()

# 4. Calculate Monthly Stats (Volume and Fraud Rate)
monthly_volume = df_july['eur_amount'].sum()
fraud_count = df_july['has_fraudulent_dispute'].sum()
total_count = len(df_july)
monthly_fraud_rate = fraud_count / total_count if total_count > 0 else 0.0

# 5. Get Merchant Static Data
merchant_info = next((m for m in merchant_data_list if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

account_type = merchant_info.get('account_type')
mcc = merchant_info.get('merchant_category_code')
capture_delay = merchant_info.get('capture_delay')

# 6. Calculate Fees
total_fees = 0.0

# Pre-process fees to ensure types are correct if needed (JSON loads as native types usually)
# Iterating through transactions
for _, tx in df_july.iterrows():
    # Build context for this specific transaction
    tx_context = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'capture_delay': capture_delay,
        'mcc': mcc,
        'is_credit': bool(tx['is_credit']),
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Find matching rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            matched_rule = rule
            break # Stop at first match
            
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee
    else:
        # If no rule matches, technically fee is 0 or undefined. 
        # In this dataset, there should always be a match (often a fallback).
        pass

# 7. Output Result
# Print just the number as requested by "What are the total fees..."
print(f"{total_fees:.14f}")

import pandas as pd
import json
import os
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
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
    return float(value) if value is not None else 0.0

def is_not_empty(array):
    """Check if array/list is not empty."""
    if array is None:
        return False
    if hasattr(array, 'size'):
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

def parse_volume_range(range_str):
    """Parses volume strings like '100k-1m', '>10m', '<10k' into (min, max)."""
    if not range_str:
        return (0, float('inf'))
    
    s = str(range_str).lower().replace(',', '').replace('€', '').strip()
    
    def parse_val(val_s):
        m = 1
        if 'k' in val_s:
            m = 1000
            val_s = val_s.replace('k', '')
        elif 'm' in val_s:
            m = 1000000
            val_s = val_s.replace('m', '')
        try:
            return float(val_s) * m
        except ValueError:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif '>' in s:
        return (parse_val(s.replace('>', '')), float('inf'))
    elif '<' in s:
        return (0, parse_val(s.replace('<', '')))
    else:
        v = parse_val(s)
        return (v, v)

def parse_fraud_range(range_str):
    """Parses fraud strings like '0%-0.5%', '>8.3%' into (min, max)."""
    if not range_str:
        return (0, float('inf'))
    
    s = str(range_str).replace('%', '').strip()
    
    if '-' in s:
        parts = s.split('-')
        return (float(parts[0])/100, float(parts[1])/100)
    elif '>' in s:
        return (float(s.replace('>', ''))/100, float('inf'))
    elif '<' in s:
        return (0, float(s.replace('<', ''))/100)
    else:
        try:
            v = float(s)/100
            return (v, v)
        except ValueError:
            return (0, float('inf'))

def match_fee_rule(ctx, rule):
    """
    Matches a transaction context against a fee rule.
    ctx: dict containing transaction and merchant details
    rule: dict containing fee rule criteria
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    if is_not_empty(rule.get('account_type')):
        if ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match)
    if is_not_empty(rule.get('merchant_category_code')):
        if ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (String/Logic match)
    if rule.get('capture_delay'):
        rd = str(rule['capture_delay'])
        md = str(ctx['capture_delay'])
        
        if rd in ['manual', 'immediate']:
            if rd != md: return False
        elif '>' in rd:
            try:
                limit = float(rd.replace('>', ''))
                if md in ['manual', 'immediate'] or float(md) <= limit: return False
            except ValueError: return False
        elif '<' in rd:
            try:
                limit = float(rd.replace('<', ''))
                if md in ['manual', 'immediate'] or float(md) >= limit: return False
            except ValueError: return False
        elif '-' in rd:
            try:
                low, high = map(float, rd.split('-'))
                if md in ['manual', 'immediate'] or not (low <= float(md) <= high): return False
            except ValueError: return False
        else:
            if rd != md: return False

    # 5. Is Credit (Bool match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False

    # 6. ACI (List match)
    if is_not_empty(rule.get('aci')):
        if ctx['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Bool match)
    if rule.get('intracountry') is not None:
        # Handle 0.0/1.0 as booleans
        rule_intra = bool(float(rule['intracountry'])) if isinstance(rule['intracountry'], (int, float)) else rule['intracountry']
        if rule_intra != ctx['intracountry']:
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_volume_range(rule['monthly_volume'])
        if not (min_v <= ctx['monthly_volume'] <= max_v):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_fraud_range(rule['monthly_fraud_level'])
        if not (min_f <= ctx['monthly_fraud_rate'] <= max_f):
            return False

    return True

# --- Main Execution ---

# File paths
input_pkl = '/output/chunk5/data/intermediate/filtered_transactions.pkl'
merchant_json_path = '/output/chunk5/data/context/merchant_data.json'
fees_json_path = '/output/chunk5/data/context/fees.json'

# Load data
print("Loading data...")
df = pd.read_pickle(input_pkl)
with open(merchant_json_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_json_path, 'r') as f:
    fees_data = json.load(f)

# 1. Get Merchant Attributes
target_merchant = 'Golfclub_Baron_Friso'
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)

if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

print(f"Merchant Info: {merchant_info}")

# 2. Calculate Monthly Stats (Volume & Fraud Rate)
# Note: Filtered DF is already Jan 2023 for this merchant
monthly_volume = df['eur_amount'].sum()
fraud_volume = df[df['has_fraudulent_dispute'] == True]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

print(f"Monthly Volume: €{monthly_volume:,.2f}")
print(f"Monthly Fraud Volume: €{fraud_volume:,.2f}")
print(f"Monthly Fraud Rate: {monthly_fraud_rate:.4%}")

# 3. Calculate Fees
total_fees = 0.0
matched_count = 0
unmatched_count = 0

for _, row in df.iterrows():
    # Determine intracountry status (Issuing == Acquirer)
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
    # Build context for matching
    ctx = {
        'card_scheme': row['card_scheme'],
        'account_type': merchant_info['account_type'],
        'mcc': merchant_info['merchant_category_code'],
        'capture_delay': merchant_info['capture_delay'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': is_intracountry,
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Find first matching rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(ctx, rule):
            matched_rule = rule
            break
    
    if matched_rule:
        # Fee formula: fixed + (rate * amount / 10000)
        # rate is an integer (e.g., 19), amount is in euros
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * row['eur_amount'] / 10000)
        total_fees += fee
        matched_count += 1
    else:
        unmatched_count += 1
        # Optional: Print first unmatched for debugging
        if unmatched_count == 1:
            print(f"Warning: No rule matched for transaction {row['psp_reference']}")
            print(f"Context: {ctx}")

print("-" * 30)
print(f"Transactions Processed: {len(df)}")
print(f"Matched Rules: {matched_count}")
print(f"Unmatched: {unmatched_count}")
print(f"Total Fees: €{total_fees:.2f}")
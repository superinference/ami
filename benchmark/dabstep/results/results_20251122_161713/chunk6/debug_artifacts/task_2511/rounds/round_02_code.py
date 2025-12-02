# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2511
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8087 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
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
        return float(v)
    return float(value)

def parse_volume_string(v_str):
    """Parses strings like '100k', '1m' to floats."""
    if not isinstance(v_str, str): return v_str
    v_str = v_str.lower().replace(',', '').replace('€', '')
    multiplier = 1
    if 'k' in v_str:
        multiplier = 1000
        v_str = v_str.replace('k', '')
    elif 'm' in v_str:
        multiplier = 1000000
        v_str = v_str.replace('m', '')
    try:
        return float(v_str) * multiplier
    except:
        return 0.0

def check_volume_rule(rule_vol, actual_vol):
    """Checks if actual volume fits the rule range string."""
    if rule_vol is None: return True
    rv = str(rule_vol).lower()
    parts = rv.split('-')
    if len(parts) == 2:
        min_v = parse_volume_string(parts[0])
        max_v = parse_volume_string(parts[1])
        return min_v <= actual_vol <= max_v
    return False

def check_fraud_rule(rule_fraud, actual_fraud):
    """Checks if actual fraud rate fits the rule range string."""
    if rule_fraud is None: return True
    rv = str(rule_fraud)
    if '>' in rv:
        limit = coerce_to_float(rv.replace('>', ''))
        return actual_fraud > limit
    if '<' in rv:
        limit = coerce_to_float(rv.replace('<', ''))
        return actual_fraud < limit
    if '-' in rv:
        parts = rv.split('-')
        min_v = coerce_to_float(parts[0])
        max_v = coerce_to_float(parts[1])
        return min_v <= actual_fraud <= max_v
    return False

def match_fee_rule(tx, rule):
    """
    Determines if a transaction matches a specific fee rule.
    tx: dict containing transaction and merchant details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx['card_scheme']:
        return False
        
    # 2. Account Type (List - Wildcard if empty)
    if rule.get('account_type'):
        if tx['account_type'] not in rule['account_type']:
            return False
            
    # 3. MCC (List - Wildcard if empty)
    if rule.get('merchant_category_code'):
        if tx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Bool - Wildcard if None)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx['is_credit']:
            return False
            
    # 5. ACI (List - Wildcard if empty)
    if rule.get('aci'):
        if tx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Bool - Wildcard if None)
    if rule.get('intracountry') is not None:
        # Intracountry logic: issuer == acquirer
        is_intra = (tx['issuing_country'] == tx['acquirer_country'])
        # JSON uses 0.0/1.0 for False/True
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 7. Capture Delay (String - Wildcard if None)
    if rule.get('capture_delay'):
        m_delay = str(tx['capture_delay'])
        r_delay = rule['capture_delay']
        
        if r_delay == 'manual':
            if m_delay != 'manual': return False
        elif r_delay == 'immediate':
            if m_delay != 'immediate': return False
        elif r_delay.startswith('>'):
            try:
                limit = float(r_delay[1:])
                if m_delay in ['manual', 'immediate']: return False
                if float(m_delay) <= limit: return False
            except: return False
        elif r_delay.startswith('<'):
            try:
                limit = float(r_delay[1:])
                if m_delay in ['manual', 'immediate']: return False
                if float(m_delay) >= limit: return False
            except: return False
        elif '-' in r_delay:
            try:
                parts = r_delay.split('-')
                min_d, max_d = float(parts[0]), float(parts[1])
                if m_delay in ['manual', 'immediate']: return False
                if not (min_d <= float(m_delay) <= max_d): return False
            except: return False
            
    # 8. Monthly Volume (Range - Wildcard if None)
    if rule.get('monthly_volume'):
        if not check_volume_rule(rule['monthly_volume'], tx['monthly_volume']):
            return False
            
    # 9. Monthly Fraud Level (Range - Wildcard if None)
    if rule.get('monthly_fraud_level'):
        if not check_fraud_rule(rule['monthly_fraud_level'], tx['monthly_fraud_level']):
            return False
            
    return True

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Rafa_AI and 2023
df_rafa = df_payments[
    (df_payments['merchant'] == 'Rafa_AI') & 
    (df_payments['year'] == 2023)
].copy()

# 3. Get Merchant Metadata
rafa_meta = next((m for m in merchant_data if m['merchant'] == 'Rafa_AI'), None)
if not rafa_meta:
    raise ValueError("Rafa_AI not found in merchant_data.json")

# 4. Get Fee Rule ID 384
fee_384 = next((f for f in fees_data if f['ID'] == 384), None)
if not fee_384:
    raise ValueError("Fee ID 384 not found in fees.json")

# 5. Calculate Monthly Stats (Volume and Fraud Rate)
# Convert day_of_year to month
df_rafa['date'] = pd.to_datetime(df_rafa['year'] * 1000 + df_rafa['day_of_year'], format='%Y%j')
df_rafa['month'] = df_rafa['date'].dt.month

monthly_stats = {}
for month in df_rafa['month'].unique():
    month_data = df_rafa[df_rafa['month'] == month]
    
    # Monthly Total Volume
    total_vol = month_data['eur_amount'].sum()
    
    # Monthly Fraud Volume (Volume of transactions marked as fraud)
    fraud_vol = month_data[month_data['has_fraudulent_dispute']]['eur_amount'].sum()
    
    # Fraud Rate (Ratio)
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'volume': total_vol,
        'fraud_rate': fraud_rate
    }

# 6. Identify Matching Transactions and Calculate Delta
matching_amount_sum = 0.0
count_matches = 0

for idx, row in df_rafa.iterrows():
    # Construct transaction context dictionary
    tx_context = {
        'card_scheme': row['card_scheme'],
        'account_type': rafa_meta['account_type'],
        'mcc': rafa_meta['merchant_category_code'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'issuing_country': row['issuing_country'],
        'acquirer_country': row['acquirer_country'],
        'capture_delay': rafa_meta['capture_delay'],
        'monthly_volume': monthly_stats[row['month']]['volume'],
        'monthly_fraud_level': monthly_stats[row['month']]['fraud_rate']
    }
    
    # Check if Fee 384 applies
    if match_fee_rule(tx_context, fee_384):
        matching_amount_sum += row['eur_amount']
        count_matches += 1

# 7. Calculate Delta
# Formula: Delta = (New Rate - Old Rate) * Total Amount / 10000
# Note: Rates are integers (e.g., 19) representing basis points/10000
old_rate = fee_384['rate']
new_rate = 1
delta = (new_rate - old_rate) * matching_amount_sum / 10000

# Output result
print(f"{delta:.14f}")

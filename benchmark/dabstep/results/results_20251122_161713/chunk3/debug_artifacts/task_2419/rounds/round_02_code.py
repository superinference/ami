# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2419
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7761 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- HELPER FUNCTIONS ---
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

def parse_range(range_str):
    """Parses strings like '>5', '100k-1m', '<3%' into (min, max) tuple."""
    if not range_str:
        return (float('-inf'), float('inf'))
    
    s = str(range_str).lower().strip().replace(',', '').replace('%', '')
    factor = 1
    if 'k' in s:
        factor = 1000
        s = s.replace('k', '')
    elif 'm' in s:
        factor = 1000000
        s = s.replace('m', '')
        
    if '-' in s:
        parts = s.split('-')
        try:
            return (float(parts[0]) * factor, float(parts[1]) * factor)
        except:
            return (float('-inf'), float('inf'))
    elif '>' in s:
        val = float(s.replace('>', '')) * factor
        return (val, float('inf'))
    elif '<' in s:
        val = float(s.replace('<', '')) * factor
        return (float('-inf'), val)
    else:
        # Exact match treated as range point
        try:
            val = float(s) * factor
            return (val, val)
        except:
            return (float('-inf'), float('inf'))

def match_fee_rule(tx_ctx, rule):
    """
    Checks if a transaction context matches a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict containing fee rule criteria
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False
        
    # 2. Account Type (Rule: list, Tx: value)
    if rule.get('account_type'):
        # Handle empty list as wildcard if necessary, though usually explicit
        if len(rule['account_type']) > 0 and tx_ctx.get('account_type') not in rule['account_type']:
            return False
            
    # 3. MCC (Rule: list, Tx: value)
    if rule.get('merchant_category_code'):
        if len(rule['merchant_category_code']) > 0 and tx_ctx.get('merchant_category_code') not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Rule: bool, Tx: bool)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False
            
    # 5. ACI (Rule: list, Tx: value)
    if rule.get('aci'):
        if len(rule['aci']) > 0 and tx_ctx.get('aci') not in rule['aci']:
            return False
            
    # 6. Intracountry (Rule: bool, Tx: bool)
    if rule.get('intracountry') is not None:
        if rule['intracountry'] != tx_ctx.get('intracountry'):
            return False
            
    # 7. Capture Delay (Rule: string, Tx: value)
    if rule.get('capture_delay'):
        r_val = str(rule['capture_delay'])
        m_val = str(tx_ctx.get('capture_delay'))
        if r_val != m_val:
            # Attempt numeric range check if rule contains operators
            try:
                if '>' in r_val:
                    limit = float(r_val.replace('>',''))
                    if float(m_val) <= limit: return False
                elif '<' in r_val:
                    limit = float(r_val.replace('<',''))
                    if float(m_val) >= limit: return False
                else:
                    return False # String mismatch
            except:
                return False # Not comparable
                
    # 8. Monthly Volume (Rule: range string, Tx: float)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        vol = tx_ctx.get('monthly_volume', 0)
        if not (min_v <= vol <= max_v):
            return False
            
    # 9. Monthly Fraud Level (Rule: range string, Tx: float 0.0-1.0)
    if rule.get('monthly_fraud_level'):
        r_str = rule['monthly_fraud_level']
        is_percent = '%' in r_str
        min_f, max_f = parse_range(r_str)
        
        f_val = tx_ctx.get('monthly_fraud_level', 0)
        # If rule was percentage (e.g. "8.3%"), parse_range returned 8.3.
        # We must convert our ratio (0.083) to percentage (8.3) for comparison.
        if is_percent:
            f_val = f_val * 100
            
        if not (min_f <= f_val <= max_f):
            return False

    return True

# --- MAIN SCRIPT ---

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant 'Rafa_AI' and Time Period 'July 2023'
# Create date column
df_payments['date'] = pd.to_datetime(df_payments['year'] * 1000 + df_payments['day_of_year'], format='%Y%j')

# Filter
rafa_txs = df_payments[
    (df_payments['merchant'] == 'Rafa_AI') & 
    (df_payments['date'].dt.month == 7) & 
    (df_payments['year'] == 2023)
].copy()

# 3. Get Merchant Metadata (Account Type, MCC, Capture Delay)
rafa_meta = next((m for m in merchant_data if m['merchant'] == 'Rafa_AI'), None)
if not rafa_meta:
    raise ValueError("Merchant Rafa_AI not found in merchant_data.json")

# 4. Calculate Monthly Stats for Rafa_AI in July 2023
# Manual: "Monthly volumes... computed always in natural months"
# Manual: "Fraud is defined as the ratio of fraudulent volume over total volume."
monthly_volume = rafa_txs['eur_amount'].sum()
fraud_volume = rafa_txs[rafa_txs['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 5. Get Fee Rule ID 276
fee_276 = next((f for f in fees_data if f['ID'] == 276), None)
if not fee_276:
    raise ValueError("Fee ID 276 not found in fees.json")

# 6. Identify Matching Transactions
# We iterate through transactions and check if they match the criteria of Fee 276.
matching_amounts = []

for _, tx in rafa_txs.iterrows():
    # Construct transaction context
    ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': rafa_meta['account_type'],
        'merchant_category_code': rafa_meta['merchant_category_code'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country'],
        'capture_delay': rafa_meta['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_rate
    }
    
    if match_fee_rule(ctx, fee_276):
        matching_amounts.append(tx['eur_amount'])

# 7. Calculate Delta
# Question: "what delta would Rafa_AI pay if the relative fee of the fee with ID=276 changed to 1?"
# Relative fee = 'rate' (basis points per 10,000)
# Old Cost = Fixed + (Old_Rate * Amount / 10000)
# New Cost = Fixed + (New_Rate * Amount / 10000)
# Delta = New Cost - Old Cost = (New_Rate - Old_Rate) * Amount / 10000

old_rate = fee_276['rate']
new_rate = 1
total_affected_amount = sum(matching_amounts)

delta = (new_rate - old_rate) * total_affected_amount / 10000

# Output result with high precision
print(f"{delta:.14f}")

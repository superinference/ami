# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1278
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 6919 characters (FULL CODE)
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
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        return float(v)
    return float(value)

def parse_range(range_str):
    """Parses strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if range_str is None:
        return (-float('inf'), float('inf'))
    
    s = str(range_str).lower().strip()
    
    def parse_val(x):
        x = x.strip()
        if x.endswith('%'):
            return float(x[:-1]) / 100
        if x.endswith('k'):
            return float(x[:-1]) * 1000
        if x.endswith('m'):
            return float(x[:-1]) * 1000000
        return float(x)

    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif s.startswith('>'):
        return (parse_val(s[1:]), float('inf'))
    elif s.startswith('<'):
        return (-float('inf'), parse_val(s[1:]))
    else:
        val = parse_val(s)
        return (val, val)

def match_fee_rule(tx_ctx, rule):
    """Checks if a transaction context matches a fee rule."""
    # 1. Card Scheme
    if rule.get('card_scheme') != tx_ctx['card_scheme']: return False
    
    # 2. Credit Status (Match specific or wildcard)
    if rule.get('is_credit') is not None and rule['is_credit'] != tx_ctx['is_credit']:
        return False

    # 3. Account Type (List match)
    if rule.get('account_type') and tx_ctx['account_type'] not in rule['account_type']:
        return False

    # 4. MCC (List match)
    if rule.get('merchant_category_code') and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False

    # 5. ACI (List match)
    if rule.get('aci') and tx_ctx['aci'] not in rule['aci']:
        return False

    # 6. Intracountry (Bool match)
    if rule.get('intracountry') is not None and rule['intracountry'] != tx_ctx['intracountry']:
        return False

    # 7. Capture Delay (Complex match)
    m_delay = str(tx_ctx['capture_delay'])
    r_delay = rule.get('capture_delay')
    if r_delay:
        if r_delay == 'immediate' and m_delay != 'immediate': return False
        if r_delay == 'manual' and m_delay != 'manual': return False
        
        # If rule is numeric range/inequality
        if m_delay not in ['immediate', 'manual']:
            try:
                days = float(m_delay)
                if r_delay == '<3' and not (days < 3): return False
                if r_delay == '>5' and not (days > 5): return False
                if r_delay == '3-5' and not (3 <= days <= 5): return False
            except ValueError:
                # Fallback for mismatch types
                if m_delay != r_delay: return False
        elif m_delay in ['immediate', 'manual'] and r_delay not in ['immediate', 'manual']:
             # String vs Numeric mismatch
             return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_ctx['monthly_volume'] <= max_v):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if not (min_f <= tx_ctx['monthly_fraud_rate'] <= max_f):
            return False

    return True

# --- Main Execution ---

# 1. Load Data
df_payments = pd.read_csv('/output/chunk6/data/context/payments.csv')
with open('/output/chunk6/data/context/fees.json') as f:
    fees = json.load(f)
with open('/output/chunk6/data/context/merchant_data.json') as f:
    merchants = json.load(f)

# 2. Prepare Merchant Lookup
merchant_lookup = {m['merchant']: m for m in merchants}

# 3. Calculate Monthly Stats (Volume & Fraud) for ALL transactions
# Create month column
df_payments['date'] = pd.to_datetime(df_payments['year'] * 1000 + df_payments['day_of_year'], format='%Y%j')
df_payments['month'] = df_payments['date'].dt.month

# Aggregate by merchant and month
monthly_stats = df_payments.groupby(['merchant', 'month']).apply(
    lambda x: pd.Series({
        'vol': x['eur_amount'].sum(),
        'fraud_vol': x[x['has_fraudulent_dispute']]['eur_amount'].sum()
    })
).reset_index()

# Calculate fraud rate
monthly_stats['fraud_rate'] = monthly_stats['fraud_vol'] / monthly_stats['vol']
monthly_stats['fraud_rate'] = monthly_stats['fraud_rate'].fillna(0)

# Create lookup dictionary: (merchant, month) -> stats
stats_lookup = {}
for _, row in monthly_stats.iterrows():
    stats_lookup[(row['merchant'], row['month'])] = {
        'vol': row['vol'],
        'fraud': row['fraud_rate']
    }

# 4. Filter Target Transactions (NexPay + Credit)
target_txs = df_payments[
    (df_payments['card_scheme'] == 'NexPay') & 
    (df_payments['is_credit'] == True)
].copy()

# 5. Filter Fees (NexPay only, we handle is_credit logic in matcher)
target_fees = [f for f in fees if f['card_scheme'] == 'NexPay']

# 6. Calculate Fees for 50 EUR
calculated_fees = []

for _, tx in target_txs.iterrows():
    merchant_name = tx['merchant']
    m_data = merchant_lookup.get(merchant_name)
    
    # Skip if merchant data missing (shouldn't happen based on schema)
    if not m_data: continue
    
    month = tx['month']
    stats = stats_lookup.get((merchant_name, month), {'vol': 0, 'fraud': 0})
    
    # Build Transaction Context
    ctx = {
        'card_scheme': 'NexPay',
        'is_credit': True,
        'account_type': m_data['account_type'],
        'mcc': m_data['merchant_category_code'],
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country'],
        'capture_delay': m_data['capture_delay'],
        'monthly_volume': stats['vol'],
        'monthly_fraud_rate': stats['fraud']
    }
    
    # Find first matching rule
    matched_fee = None
    for rule in target_fees:
        if match_fee_rule(ctx, rule):
            # Fee = Fixed + (Rate * Amount / 10000)
            # Amount is fixed at 50 EUR per question
            fee = rule['fixed_amount'] + (rule['rate'] * 50.0 / 10000.0)
            matched_fee = fee
            break
            
    if matched_fee is not None:
        calculated_fees.append(matched_fee)

# 7. Compute and Print Average
if calculated_fees:
    avg_fee = sum(calculated_fees) / len(calculated_fees)
    print(f"{avg_fee:.14f}")
else:
    print("No applicable fees found.")

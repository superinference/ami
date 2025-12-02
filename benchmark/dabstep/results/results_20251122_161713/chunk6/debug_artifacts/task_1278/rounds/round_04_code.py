# ═══════════════════════════════════════════════════════════
# Round 4 - Task 1278
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 4
# Code length: 8290 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---
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
        try:
            val = parse_val(s)
            return (val, val)
        except:
            return (-float('inf'), float('inf'))

def match_fee_rule(tx_ctx, rule):
    """Checks if a transaction context matches a fee rule."""
    # 1. Card Scheme
    if rule.get('card_scheme') != tx_ctx['card_scheme']: return False
    
    # 2. Credit Status (Match specific or wildcard)
    # If rule has is_credit=None, it matches both. If specific, must match.
    if rule.get('is_credit') is not None and rule['is_credit'] != tx_ctx['is_credit']:
        return False

    # 3. Account Type (List match: empty list = all, otherwise must contain)
    if rule.get('account_type') and tx_ctx['account_type'] not in rule['account_type']:
        return False

    # 4. MCC (List match: empty list = all, otherwise must contain)
    if rule.get('merchant_category_code') and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False

    # 5. ACI (List match: empty list = all, otherwise must contain)
    if rule.get('aci') and tx_ctx['aci'] not in rule['aci']:
        return False

    # 6. Intracountry (Bool match: None = all, otherwise must match)
    if rule.get('intracountry') is not None and rule['intracountry'] != tx_ctx['intracountry']:
        return False

    # 7. Capture Delay (Complex match)
    m_delay = str(tx_ctx['capture_delay'])
    r_delay = rule.get('capture_delay')
    
    if r_delay:
        # Exact string matches
        if r_delay == 'immediate':
            if m_delay != 'immediate': return False
        elif r_delay == 'manual':
            if m_delay != 'manual': return False
        elif m_delay in ['immediate', 'manual']:
            # If merchant is string but rule is numeric range (e.g. <3), it's a mismatch
            return False
        else:
            # Numeric comparison
            try:
                days = float(m_delay)
                min_d, max_d = parse_range(r_delay)
                # Handle open ranges correctly
                if r_delay.startswith('<'):
                    if not (days < max_d): return False
                elif r_delay.startswith('>'):
                    if not (days > min_d): return False
                else:
                    if not (min_d <= days <= max_d): return False
            except ValueError:
                return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_ctx['monthly_volume'] <= max_v):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        # Handle edge case where fraud rate is exactly on boundary or float precision
        # Using a small epsilon for float comparison if needed, but standard <= usually works
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
# Create date/month columns
df_payments['date'] = pd.to_datetime(df_payments['year'] * 1000 + df_payments['day_of_year'], format='%Y%j')
df_payments['month'] = df_payments['date'].dt.month

# Aggregate by merchant and month
# Manual: "fraud levels measured as ratio between monthly total volume and monthly volume notified as fraud"
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

# 5. Filter Fees (NexPay only)
# We keep rules where is_credit is True OR None (wildcard)
target_fees = [f for f in fees if f['card_scheme'] == 'NexPay' and (f['is_credit'] is True or f['is_credit'] is None)]

# 6. Calculate Fees for 50 EUR
calculated_fees = []
transaction_amount_for_fee = 50.0

# Optimization: Group transactions by unique characteristics to speed up matching
# Characteristics that affect fee matching: merchant, aci, intracountry, month
target_txs['intracountry'] = target_txs['issuing_country'] == target_txs['acquirer_country']

# Group by relevant columns
grouped_txs = target_txs.groupby(['merchant', 'month', 'aci', 'intracountry']).size().reset_index(name='count')

for _, group in grouped_txs.iterrows():
    merchant_name = group['merchant']
    month = group['month']
    aci = group['aci']
    intracountry = group['intracountry']
    count = group['count']
    
    m_data = merchant_lookup.get(merchant_name)
    if not m_data: continue
    
    stats = stats_lookup.get((merchant_name, month), {'vol': 0, 'fraud': 0})
    
    # Build Transaction Context
    ctx = {
        'card_scheme': 'NexPay',
        'is_credit': True,
        'account_type': m_data['account_type'],
        'mcc': m_data['merchant_category_code'],
        'aci': aci,
        'intracountry': intracountry,
        'capture_delay': m_data['capture_delay'],
        'monthly_volume': stats['vol'],
        'monthly_fraud_rate': stats['fraud']
    }
    
    # Find first matching rule
    matched_fee = None
    for rule in target_fees:
        if match_fee_rule(ctx, rule):
            # Fee = Fixed + (Rate * Amount / 10000)
            fee = rule['fixed_amount'] + (rule['rate'] * transaction_amount_for_fee / 10000.0)
            matched_fee = fee
            break
            
    if matched_fee is not None:
        # Add the fee for EACH transaction in this group
        calculated_fees.extend([matched_fee] * count)

# 7. Compute and Print Average
if calculated_fees:
    avg_fee = sum(calculated_fees) / len(calculated_fees)
    print(f"{avg_fee:.14f}")
else:
    print("No applicable fees found.")

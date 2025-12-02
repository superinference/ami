# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2477
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 6920 characters (FULL CODE)
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
    """Parses a range string like '100k-1m' or '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    is_percent = '%' in s
    s = s.replace('%', '')
    
    def parse_val(val_s):
        val_s = val_s.strip()
        mult = 1
        if val_s.endswith('k'):
            mult = 1000
            val_s = val_s[:-1]
        elif val_s.endswith('m'):
            mult = 1000000
            val_s = val_s[:-1]
        try:
            v = float(val_s) * mult
            return v / 100 if is_percent else v
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('>'):
        return parse_val(s[1:]), float('inf')
    elif s.startswith('<'):
        return 0.0, parse_val(s[1:])
    else:
        v = parse_val(s)
        return v, v

def match_fee_rule(tx, rule):
    """
    Checks if a transaction matches a fee rule.
    tx: dict containing transaction details + merchant details + monthly stats
    rule: dict containing fee rule details
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx.get('card_scheme'):
        return False

    # 2. Account Type
    if rule.get('account_type'):
        if tx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code
    if rule.get('merchant_category_code'):
        if tx.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx.get('is_credit'):
            return False

    # 5. ACI
    if rule.get('aci'):
        if tx.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry
    if rule.get('intracountry') is not None:
        if rule['intracountry'] != tx.get('intracountry'):
            return False

    # 7. Monthly Volume
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        vol = tx.get('monthly_volume', 0)
        if not (min_v <= vol <= max_v):
            return False

    # 8. Monthly Fraud Level
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        fraud = tx.get('monthly_fraud_rate', 0)
        if not (min_f <= fraud <= max_f):
            return False
            
    # 9. Capture Delay
    if rule.get('capture_delay'):
        cd_rule = rule['capture_delay']
        cd_merch = str(tx.get('capture_delay', ''))
        if cd_rule.startswith('>'):
            try:
                limit = float(cd_rule[1:])
                val = float(cd_merch) if cd_merch.replace('.','',1).isdigit() else 0
                if not (val > limit): return False
            except:
                pass 
        elif cd_rule.startswith('<'):
            try:
                limit = float(cd_rule[1:])
                val = float(cd_merch) if cd_merch.replace('.','',1).isdigit() else 0
                if not (val < limit): return False
            except:
                pass
        elif cd_rule != cd_merch:
             return False

    return True

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Target Setup
target_merchant = 'Belles_cookbook_store'
target_year = 2023
target_fee_id = 276
new_rate = 1

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 4. Get Target Fee Rule
target_fee_rule = next((f for f in fees_data if f['ID'] == target_fee_id), None)
if not target_fee_rule:
    raise ValueError(f"Fee ID {target_fee_id} not found in fees.json")

old_rate = target_fee_rule['rate']

# 5. Filter Transactions
df_filtered = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

# 6. Calculate Monthly Stats (Volume & Fraud)
# Map day_of_year to month (approximate using standard year)
def get_month(day):
    return (pd.to_datetime(day-1, unit='D', origin=str(target_year)).month)

df_filtered['month'] = df_filtered['day_of_year'].apply(get_month)

monthly_stats = {}
for month in df_filtered['month'].unique():
    month_txs = df_filtered[df_filtered['month'] == month]
    vol = month_txs['eur_amount'].sum()
    
    # Fraud defined as ratio of fraudulent volume over total volume (Manual Sec 7)
    fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate_vol = (fraud_vol / vol) if vol > 0 else 0.0
    
    monthly_stats[month] = {
        'volume': vol,
        'fraud_rate': fraud_rate_vol
    }

# 7. Identify Matching Transactions and Calculate Delta
affected_amount_sum = 0.0
matching_count = 0

for idx, row in df_filtered.iterrows():
    # Build transaction dict for matching
    tx_dict = row.to_dict()
    tx_dict['account_type'] = merchant_info['account_type']
    tx_dict['mcc'] = merchant_info['merchant_category_code']
    tx_dict['capture_delay'] = merchant_info['capture_delay']
    
    # Intracountry: Issuer == Acquirer (Manual Sec 5.1.1)
    tx_dict['intracountry'] = (row['issuing_country'] == row['acquirer_country'])
    
    # Monthly stats
    m_stats = monthly_stats.get(row['month'], {'volume': 0, 'fraud_rate': 0})
    tx_dict['monthly_volume'] = m_stats['volume']
    tx_dict['monthly_fraud_rate'] = m_stats['fraud_rate']
    
    # Check match
    if match_fee_rule(tx_dict, target_fee_rule):
        affected_amount_sum += row['eur_amount']
        matching_count += 1

# 8. Calculate Delta
# Delta = (NewRate - OldRate) * Amt / 10000
delta = (new_rate - old_rate) * affected_amount_sum / 10000.0

print(f"{delta:.14f}")

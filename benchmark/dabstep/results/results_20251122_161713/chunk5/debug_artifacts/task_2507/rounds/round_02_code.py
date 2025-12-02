# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2507
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7667 characters (FULL CODE)
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
    return 0.0

def parse_volume_range(vol_str):
    """Parses volume strings like '100k-1m' into (min, max)."""
    if not vol_str:
        return 0, float('inf')
    
    v = vol_str.lower().replace(',', '').replace('€', '').replace('$', '')
    
    def parse_val(s):
        mul = 1
        if 'k' in s:
            mul = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mul = 1000000
            s = s.replace('m', '')
        try:
            return float(s) * mul
        except:
            return 0.0

    if '-' in v:
        parts = v.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif '>' in v:
        return parse_val(v.replace('>', '')), float('inf')
    elif '<' in v:
        return 0, parse_val(v.replace('<', ''))
    return 0, float('inf')

def parse_fraud_range(fraud_str):
    """Parses fraud strings like '7.7%-8.3%' into (min, max)."""
    if not fraud_str:
        return 0.0, 1.0
    
    v = fraud_str.replace('%', '')
    
    if '-' in v:
        parts = v.split('-')
        return float(parts[0])/100, float(parts[1])/100
    elif '>' in v:
        return float(v.replace('>', ''))/100, 1.0
    elif '<' in v:
        return 0.0, float(v.replace('<', ''))/100
    return 0.0, 1.0

def match_fee_rule(tx_dict, rule):
    """
    Determines if a fee rule applies to a transaction.
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_dict.get('card_scheme'):
        return False
        
    # 2. Account Type (Wildcard: [] or None)
    if rule.get('account_type'):
        if tx_dict.get('account_type') not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (Wildcard: [] or None)
    if rule.get('merchant_category_code'):
        if tx_dict.get('mcc') not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay
    if rule.get('capture_delay'):
        m_delay = str(tx_dict.get('capture_delay', ''))
        r_delay = rule['capture_delay']
        
        if r_delay == 'immediate' and m_delay != 'immediate': return False
        if r_delay == 'manual' and m_delay != 'manual': return False
        
        try:
            days = float(m_delay)
            if r_delay == '<3' and not (days < 3): return False
            if r_delay == '>5' and not (days > 5): return False
            if r_delay == '3-5' and not (3 <= days <= 5): return False
        except ValueError:
            if r_delay in ['<3', '>5', '3-5'] and m_delay in ['immediate', 'manual']:
                return False

    # 5. Is Credit
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_dict.get('is_credit'):
            return False

    # 6. ACI
    if rule.get('aci'):
        if tx_dict.get('aci') not in rule['aci']:
            return False

    # 7. Intracountry
    if rule.get('intracountry') is not None:
        is_intra = (tx_dict.get('issuing_country') == tx_dict.get('acquirer_country'))
        rule_intra = rule['intracountry']
        if isinstance(rule_intra, float):
            rule_intra = bool(rule_intra)
        if rule_intra != is_intra:
            return False

    # 8. Monthly Volume
    if rule.get('monthly_volume'):
        min_v, max_v = parse_volume_range(rule['monthly_volume'])
        vol = tx_dict.get('monthly_volume_eur', 0)
        if not (min_v <= vol <= max_v):
            return False

    # 9. Monthly Fraud Level
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_fraud_range(rule['monthly_fraud_level'])
        fraud = tx_dict.get('monthly_fraud_rate', 0)
        if not (min_f <= fraud <= max_f):
            return False

    return True

# --- MAIN SCRIPT ---

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Target Merchant and Year
target_merchant = "Martinis_Fine_Steakhouse"
df_target = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == 2023)
].copy()

# 3. Get Merchant Metadata
m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not m_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 4. Calculate Monthly Stats (Volume & Fraud)
# Map day_of_year to month (2023 is non-leap)
df_target['date'] = pd.to_datetime(df_target['year'] * 1000 + df_target['day_of_year'], format='%Y%j')
df_target['month'] = df_target['date'].dt.month

# Calculate Fraud Volume (Amount where has_fraudulent_dispute is True)
df_target['fraud_amount'] = df_target['eur_amount'] * df_target['has_fraudulent_dispute'].astype(int)

monthly_stats = df_target.groupby('month').agg(
    total_vol=('eur_amount', 'sum'),
    fraud_vol=('fraud_amount', 'sum')
).reset_index()

# Fraud Rate = Fraud Volume / Total Volume
monthly_stats['fraud_rate'] = monthly_stats['fraud_vol'] / monthly_stats['total_vol']
month_lookup = monthly_stats.set_index('month').to_dict('index')

# 5. Identify Target Fee Rule (ID=276)
target_rule_id = 276
target_rule = next((r for r in fees_data if r['ID'] == target_rule_id), None)
if not target_rule:
    print("Fee ID 276 not found.")
    exit()

original_rate = target_rule['rate']
new_rate = 1
rate_diff = new_rate - original_rate

# 6. Iterate Transactions and Calculate Delta
total_delta = 0.0

# Pre-process merchant info
m_account_type = m_info.get('account_type')
m_mcc = m_info.get('merchant_category_code')
m_capture_delay = m_info.get('capture_delay')

for idx, row in df_target.iterrows():
    month = row['month']
    stats = month_lookup.get(month, {'total_vol': 0, 'fraud_rate': 0})
    
    tx_dict = {
        'card_scheme': row['card_scheme'],
        'account_type': m_account_type,
        'mcc': m_mcc,
        'capture_delay': m_capture_delay,
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'issuing_country': row['issuing_country'],
        'acquirer_country': row['acquirer_country'],
        'monthly_volume_eur': stats['total_vol'],
        'monthly_fraud_rate': stats['fraud_rate']
    }
    
    # Find the FIRST matching rule (Priority based on list order)
    winning_rule_id = None
    for rule in fees_data:
        if match_fee_rule(tx_dict, rule):
            winning_rule_id = rule['ID']
            break
            
    # If the winning rule is our target rule, calculate delta
    if winning_rule_id == target_rule_id:
        # Delta = (New Rate - Old Rate) * Amount / 10000
        delta = (rate_diff * row['eur_amount']) / 10000.0
        total_delta += delta

# 7. Output Result
print(f"{total_delta:.14f}")

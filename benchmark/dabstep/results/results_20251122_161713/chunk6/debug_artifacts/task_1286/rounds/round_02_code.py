# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1286
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7365 characters (FULL CODE)
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
        except:
            return 0.0
    return 0.0

def get_month(doy):
    """Convert day_of_year to month (1-12) for 2023 (non-leap)."""
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cum_days = np.cumsum(days_in_months)
    for i, d in enumerate(cum_days):
        if doy <= d:
            return i + 1
    return 12

def parse_vol_str(s):
    """Parse volume strings like '100k', '1m'."""
    if not isinstance(s, str): return 0
    s = s.lower().replace(',', '')
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
        return 0

def check_volume_match(rule_vol, actual_vol):
    """Check if actual volume falls within rule range."""
    if rule_vol is None: return True
    s = str(rule_vol).lower()
    if '-' in s:
        low, high = s.split('-')
        l = parse_vol_str(low)
        h = parse_vol_str(high)
        return l <= actual_vol <= h
    if '>' in s:
        val = parse_vol_str(s.replace('>', ''))
        return actual_vol > val
    if '<' in s:
        val = parse_vol_str(s.replace('<', ''))
        return actual_vol < val
    return False

def check_fraud_match(rule_fraud, actual_fraud_rate):
    """Check if actual fraud rate (ratio) falls within rule range (string %)."""
    if rule_fraud is None: return True
    s = str(rule_fraud).replace('%', '')
    
    def parse_pct(p_str):
        p_str = p_str.strip()
        if not p_str: return 0.0
        return float(p_str) / 100.0

    if '-' in s:
        low, high = s.split('-')
        l = parse_pct(low)
        h = parse_pct(high)
        return l <= actual_fraud_rate <= h
    if '>' in s:
        val = parse_pct(s.replace('>', ''))
        return actual_fraud_rate > val
    if '<' in s:
        val = parse_pct(s.replace('<', ''))
        return actual_fraud_rate < val
    return False

def check_capture_match(rule_cap, actual_cap):
    """Check if actual capture delay matches rule."""
    if rule_cap is None: return True
    if str(rule_cap) == str(actual_cap): return True
    
    # Handle special keywords
    if actual_cap in ['immediate', 'manual']:
        return str(rule_cap) == str(actual_cap)
        
    # Handle numeric comparisons
    try:
        ac_val = float(actual_cap)
    except:
        return False
        
    if '<' in rule_cap:
        thresh = float(rule_cap.replace('<', ''))
        return ac_val < thresh
    if '>' in rule_cap:
        thresh = float(rule_cap.replace('>', ''))
        return ac_val > thresh
    if '-' in rule_cap:
        l, h = map(float, rule_cap.split('-'))
        return l <= ac_val <= h
        
    return False

def match_fee_rule(ctx, rule):
    """Determine if a fee rule applies to a transaction context."""
    # 1. Card Scheme
    if rule['card_scheme'] != ctx['card_scheme']: return False
    
    # 2. Is Credit (bool or None)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != ctx['is_credit']: return False
        
    # 3. Intracountry (bool or None)
    if rule['intracountry'] is not None:
        # JSON has 0.0 (False) or 1.0 (True)
        rule_intra = bool(rule['intracountry'])
        if rule_intra != ctx['intracountry']: return False
        
    # 4. Account Type (list or empty)
    if rule['account_type']:
        if ctx['account_type'] not in rule['account_type']: return False
        
    # 5. MCC (list or empty)
    if rule['merchant_category_code']:
        if ctx['merchant_category_code'] not in rule['merchant_category_code']: return False
        
    # 6. ACI (list or empty)
    if rule['aci']:
        if ctx['aci'] not in rule['aci']: return False
        
    # 7. Capture Delay
    if not check_capture_match(rule['capture_delay'], ctx['capture_delay']): return False
    
    # 8. Monthly Volume
    if not check_volume_match(rule['monthly_volume'], ctx['monthly_volume']): return False
    
    # 9. Monthly Fraud
    if not check_fraud_match(rule['monthly_fraud_level'], ctx['monthly_fraud_level']): return False
    
    return True

# --- MAIN EXECUTION ---

# 1. Load Data
df_pay = pd.read_csv('/output/chunk6/data/context/payments.csv')
with open('/output/chunk6/data/context/fees.json') as f:
    fees_data = json.load(f)
with open('/output/chunk6/data/context/merchant_data.json') as f:
    merch_data = json.load(f)

# 2. Prepare Merchant Lookup
merch_lookup = {m['merchant']: m for m in merch_data}

# 3. Calculate Monthly Stats (Volume & Fraud)
# Map day_of_year to month
df_pay['month'] = df_pay['day_of_year'].apply(get_month)

# Group by merchant and month
stats = df_pay.groupby(['merchant', 'month']).agg(
    vol=('eur_amount', 'sum'),
    cnt=('eur_amount', 'count'),
    fraud=('has_fraudulent_dispute', 'sum')
).reset_index()

stats['fraud_rate'] = stats['fraud'] / stats['cnt']

# Create fast lookup: (merchant, month) -> (volume, fraud_rate)
stats_map = {}
for _, r in stats.iterrows():
    stats_map[(r['merchant'], r['month'])] = (r['vol'], r['fraud_rate'])

# 4. Filter Target Transactions
# Question: "For credit transactions... NexPay"
target_df = df_pay[
    (df_pay['card_scheme'] == 'NexPay') & 
    (df_pay['is_credit'] == True)
].copy()

# 5. Calculate Fees for 500 EUR
calculated_fees = []
transaction_amount = 500.0

for _, tx in target_df.iterrows():
    m_name = tx['merchant']
    m_info = merch_lookup.get(m_name)
    
    if not m_info:
        continue
        
    # Retrieve monthly stats for this transaction's merchant and month
    vol, fraud_r = stats_map.get((m_name, tx['month']), (0, 0))
    
    # Build Context
    ctx = {
        'card_scheme': 'NexPay',
        'is_credit': True,
        'intracountry': tx['issuing_country'] == tx['acquirer_country'],
        'account_type': m_info['account_type'],
        'merchant_category_code': m_info['merchant_category_code'],
        'aci': tx['aci'],
        'capture_delay': m_info['capture_delay'],
        'monthly_volume': vol,
        'monthly_fraud_level': fraud_r
    }
    
    # Find First Matching Rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(ctx, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Calculate Fee: Fixed + (Rate * Amount / 10000)
        # Rate is in basis points (per 10,000)
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * transaction_amount / 10000.0)
        calculated_fees.append(fee)

# 6. Output Result
if calculated_fees:
    avg_fee = np.mean(calculated_fees)
    print(f"{avg_fee:.14f}")
else:
    print("0.0")

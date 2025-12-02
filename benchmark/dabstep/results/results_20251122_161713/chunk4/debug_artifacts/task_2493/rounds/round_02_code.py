# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2493
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8354 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        if v.endswith('k'):
            try:
                return float(v[:-1]) * 1000
            except:
                pass
        if v.endswith('m'):
            try:
                return float(v[:-1]) * 1000000
            except:
                pass
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except:
                pass
        try:
            return float(v)
        except:
            return 0.0
    return 0.0

def parse_range_check(value, rule_str):
    """Check if value matches a rule string like '100k-1m', '>5', '<3'."""
    if rule_str is None:
        return True
    
    # Clean rule string
    s = str(rule_str).strip().lower()
    
    # Handle inequalities
    if s.startswith('>'):
        limit = coerce_to_float(s[1:])
        return value > limit
    if s.startswith('<'):
        limit = coerce_to_float(s[1:])
        return value < limit
    if s.startswith('≥'):
        limit = coerce_to_float(s[1:])
        return value >= limit
    if s.startswith('≤'):
        limit = coerce_to_float(s[1:])
        return value <= limit
        
    # Handle ranges
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            low = coerce_to_float(parts[0])
            high = coerce_to_float(parts[1])
            return low <= value <= high
            
    # Handle exact match (numeric)
    try:
        target = coerce_to_float(s)
        return value == target
    except:
        return False

def match_capture_delay(merchant_delay, rule_delay):
    """Match merchant capture delay (str) against rule (str)."""
    m_val = str(merchant_delay).lower()
    r_val = str(rule_delay).lower()
    
    # Exact categorical match
    if r_val in ['immediate', 'manual']:
        return m_val == r_val
        
    # If rule is numeric/range, convert merchant val
    # 'immediate' -> 0
    # 'manual' -> None (fail numeric checks)
    m_num = None
    if m_val == 'immediate':
        m_num = 0
    elif m_val == 'manual':
        m_num = None
    else:
        try:
            m_num = float(m_val)
        except:
            m_num = None
            
    if m_num is None:
        # Cannot compare manual/unknown to numeric rule like '<3'
        return False 
        
    return parse_range_check(m_num, r_val)

def match_fee_rule(ctx, rule):
    """
    Check if a transaction context matches a fee rule.
    ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact)
    if rule.get('card_scheme') != ctx.get('card_scheme'):
        return False
        
    # 2. Account Type (List contains)
    if rule.get('account_type'):
        if ctx.get('account_type') not in rule['account_type']:
            return False
            
    # 3. MCC (List contains)
    if rule.get('merchant_category_code'):
        if ctx.get('mcc') not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Exact bool)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx.get('is_credit'):
            return False
            
    # 5. ACI (List contains)
    if rule.get('aci'):
        if ctx.get('aci') not in rule['aci']:
            return False
            
    # 6. Intracountry (Exact bool)
    if rule.get('intracountry') is not None:
        if rule['intracountry'] != ctx.get('intracountry'):
            return False
            
    # 7. Capture Delay (Complex)
    if rule.get('capture_delay'):
        if not match_capture_delay(ctx.get('capture_delay'), rule['capture_delay']):
            return False
            
    # 8. Monthly Volume (Range)
    if rule.get('monthly_volume'):
        if not parse_range_check(ctx.get('monthly_volume'), rule['monthly_volume']):
            return False
            
    # 9. Monthly Fraud Level (Range)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(ctx.get('monthly_fraud_level'), rule['monthly_fraud_level']):
            return False
            
    return True

def get_month_from_doy(doy):
    """Convert day of year (1-365) to month (1-12) for non-leap year."""
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cumulative = 0
    for i, days in enumerate(days_in_months):
        cumulative += days
        if doy <= cumulative:
            return i + 1
    return 12

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# File Paths
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Target Merchant and Year
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023

df_filtered = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

# 3. Get Merchant Metadata
m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not m_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

mcc = m_info['merchant_category_code']
account_type = m_info['account_type']
capture_delay = m_info['capture_delay']

# 4. Calculate Monthly Stats (Volume & Fraud)
df_filtered['month'] = df_filtered['day_of_year'].apply(get_month_from_doy)

monthly_stats = {}
for month in range(1, 13):
    month_txs = df_filtered[df_filtered['month'] == month]
    if len(month_txs) == 0:
        monthly_stats[month] = {'vol': 0, 'fraud_rate': 0}
        continue
        
    total_vol = month_txs['eur_amount'].sum()
    fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {'vol': total_vol, 'fraud_rate': fraud_rate}

# 5. Identify Target Fee Rule (ID=64)
target_rule_id = 64
target_rule = next((r for r in fees_data if r['ID'] == target_rule_id), None)
if not target_rule:
    raise ValueError(f"Fee rule ID {target_rule_id} not found.")

old_rate = target_rule['rate']
new_rate = 1

# 6. Iterate Transactions and Calculate Delta
total_delta = 0.0
affected_count = 0

# Use fees list order as priority
sorted_fees = fees_data 

for idx, row in df_filtered.iterrows():
    # Build Context
    month = row['month']
    stats = monthly_stats.get(month, {'vol': 0, 'fraud_rate': 0})
    
    # Intracountry check
    # Handle potential NaNs safely
    issuing = str(row['issuing_country']) if pd.notna(row['issuing_country']) else ''
    acquirer = str(row['acquirer_country']) if pd.notna(row['acquirer_country']) else ''
    is_intracountry = (issuing == acquirer) and (issuing != '')
    
    ctx = {
        'card_scheme': row['card_scheme'],
        'account_type': account_type,
        'mcc': mcc,
        'is_credit': bool(row['is_credit']),
        'aci': row['aci'],
        'intracountry': is_intracountry,
        'capture_delay': capture_delay,
        'monthly_volume': stats['vol'],
        'monthly_fraud_level': stats['fraud_rate']
    }
    
    # Find Active Rule (First Match)
    active_rule_id = None
    for rule in sorted_fees:
        if match_fee_rule(ctx, rule):
            active_rule_id = rule['ID']
            break
            
    # Check if Active Rule is Target Rule
    if active_rule_id == target_rule_id:
        amount = row['eur_amount']
        # Delta = (New Rate - Old Rate) * Amount / 10000
        delta = (new_rate - old_rate) * amount / 10000.0
        total_delta += delta
        affected_count += 1

# Print result with high precision
print(f"{total_delta:.14f}")

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2545
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10116 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

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

def parse_k_m(s):
    """Parse strings like '100k', '1m' into floats."""
    if not isinstance(s, str): return float(s)
    s = s.lower().strip().replace(',', '')
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
        return 0.0

def parse_range_string(s, is_percentage=False):
    """Parses range strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if s is None:
        return (-float('inf'), float('inf'))
    
    s = str(s).strip()
    
    # Handle percentage conversion
    scale = 0.01 if is_percentage else 1.0
    clean_s = s.replace('%', '')
    
    try:
        if '-' in clean_s:
            parts = clean_s.split('-')
            low = parse_k_m(parts[0]) * scale
            high = parse_k_m(parts[1]) * scale
            return (low, high)
        elif s.startswith('>'):
            val = parse_k_m(clean_s.replace('>', '')) * scale
            return (val, float('inf')) # Exclusive > handled as inclusive for simplicity or epsilon adjustment? 
            # Usually >5 means >= 5.00001, but for discrete steps often treated as boundary.
            # Let's assume strict inequality logic in matcher or inclusive here.
            # Given standard fee tiers, usually inclusive on one end. 
            # Let's return (val, inf) and handle strictness in matcher if needed.
            # Actually, standard interpretation: >5 means x > 5.
        elif s.startswith('<'):
            val = parse_k_m(clean_s.replace('<', '')) * scale
            return (-float('inf'), val)
        else:
            # Exact value? Unlikely for these fields, but possible
            val = parse_k_m(clean_s) * scale
            return (val, val)
    except:
        return (-float('inf'), float('inf'))

def match_capture_delay(merchant_val, rule_val):
    """Matches merchant capture delay (str/int) against rule (str/range)."""
    if rule_val is None: return True
    
    # Direct string match (e.g., 'immediate', 'manual')
    if str(merchant_val).lower() == str(rule_val).lower(): return True
    
    # Numeric comparison
    try:
        m_days = float(merchant_val)
    except ValueError:
        # merchant_val is non-numeric (e.g. 'immediate') but rule didn't match above
        return False
        
    # Parse rule_val logic
    rule_val = str(rule_val).lower()
    if rule_val.startswith('<'):
        limit = float(rule_val.replace('<', ''))
        return m_days < limit
    if rule_val.startswith('>'):
        limit = float(rule_val.replace('>', ''))
        return m_days > limit
    if '-' in rule_val:
        parts = rule_val.split('-')
        low = float(parts[0])
        high = float(parts[1])
        return low <= m_days <= high
        
    return False

def get_fee(ctx, fees_rules):
    """Finds the first matching fee rule and calculates the fee."""
    for rule in fees_rules:
        # 1. Card Scheme (Exact match)
        if rule['card_scheme'] != ctx['card_scheme']:
            continue
            
        # 2. Account Type (List match)
        if rule['account_type'] and ctx['account_type'] not in rule['account_type']:
            continue
            
        # 3. Capture Delay (Complex match)
        if not match_capture_delay(ctx['capture_delay'], rule['capture_delay']):
            continue
            
        # 4. Monthly Fraud Level (Range match)
        # Note: rule['_fraud_range'] is pre-parsed as (min, max)
        # Handling strict inequalities for ranges like >8.3%
        f_min, f_max = rule['_fraud_range']
        # Logic: If rule was ">8.3%", parsed as (0.083, inf). 
        # We need to check if it's strictly greater or inclusive.
        # Standard implementation: Inclusive of bounds for "X-Y", exclusive for >/< usually.
        # However, for simplicity and robustness with floats:
        # If rule string had '>', use >. If '<', use <. Else <=.
        # Let's use the raw string logic stored in rule for precision if needed, 
        # or rely on the pre-parsed tuple with epsilon.
        # Let's re-implement simple range check based on tuple:
        val = ctx['monthly_fraud_level']
        
        # Refined check based on original string to handle boundary conditions correctly
        raw_fraud = rule['monthly_fraud_level']
        if raw_fraud:
            if '>' in raw_fraud and not (val > f_min): continue
            elif '<' in raw_fraud and not (val < f_max): continue
            elif '-' in raw_fraud and not (f_min <= val <= f_max): continue
        
        # 5. Monthly Volume (Range match)
        v_min, v_max = rule['_vol_range']
        vol = ctx['monthly_volume']
        raw_vol = rule['monthly_volume']
        if raw_vol:
            if '>' in raw_vol and not (vol > v_min): continue
            elif '<' in raw_vol and not (vol < v_max): continue
            elif '-' in raw_vol and not (v_min <= vol <= v_max): continue

        # 6. Merchant Category Code (List match)
        if rule['merchant_category_code'] and ctx['mcc'] not in rule['merchant_category_code']:
            continue
            
        # 7. Is Credit (Bool match)
        if rule['is_credit'] is not None and rule['is_credit'] != ctx['is_credit']:
            continue
            
        # 8. ACI (List match)
        if rule['aci'] and ctx['aci'] not in rule['aci']:
            continue
            
        # 9. Intracountry (Bool match)
        if rule['intracountry'] is not None:
            rule_intra = bool(rule['intracountry'])
            if rule_intra != ctx['intracountry']:
                continue
                
        # MATCH FOUND
        # Fee = fixed + (rate * amount / 10000)
        return rule['fixed_amount'] + (rule['rate'] * ctx['amount'] / 10000.0)
        
    return 0.0

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant and Year
target_merchant = 'Martinis_Fine_Steakhouse'
target_year = 2023
df = df_payments[(df_payments['merchant'] == target_merchant) & (df_payments['year'] == target_year)].copy()

# 3. Get Merchant Context
m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not m_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

orig_mcc = m_info['merchant_category_code']
new_mcc = 5911
account_type = m_info['account_type']
capture_delay = m_info['capture_delay']

# 4. Calculate Monthly Stats (Volume & Fraud)
# Map day_of_year to month
# 2023 is not a leap year
df['date'] = pd.to_datetime(df['year'] * 1000 + df['day_of_year'], format='%Y%j')
df['month'] = df['date'].dt.month

monthly_stats = {}
months = df['month'].unique()
for m in months:
    m_df = df[df['month'] == m]
    total_vol = m_df['eur_amount'].sum()
    fraud_vol = m_df[m_df['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    
    # Fraud rate = Fraud Volume / Total Volume
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[m] = {
        'vol': total_vol,
        'fraud_rate': fraud_rate
    }

# 5. Pre-process Fee Rules (Parse ranges once)
processed_fees = []
for rule in fees_data:
    # Parse Volume
    rule['_vol_range'] = parse_range_string(rule['monthly_volume'], is_percentage=False)
    # Parse Fraud
    rule['_fraud_range'] = parse_range_string(rule['monthly_fraud_level'], is_percentage=True)
    processed_fees.append(rule)

# 6. Calculate Fees and Delta
total_delta = 0.0

# Iterate through transactions
# Using itertuples for speed, though iterrows is fine for this volume
for row in df.itertuples():
    # Determine Intracountry
    # issuing_country == acquirer_country
    is_intra = (row.issuing_country == row.acquirer_country)
    
    # Get Monthly Stats
    stats = monthly_stats[row.month]
    
    # Build Context
    ctx = {
        'card_scheme': row.card_scheme,
        'account_type': account_type,
        'capture_delay': capture_delay,
        'monthly_volume': stats['vol'],
        'monthly_fraud_level': stats['fraud_rate'],
        'is_credit': row.is_credit,
        'aci': row.aci,
        'intracountry': is_intra,
        'amount': row.eur_amount,
        'mcc': None # Placeholder
    }
    
    # Calculate Old Fee
    ctx['mcc'] = orig_mcc
    fee_old = get_fee(ctx, processed_fees)
    
    # Calculate New Fee
    ctx['mcc'] = new_mcc
    fee_new = get_fee(ctx, processed_fees)
    
    # Accumulate Delta
    total_delta += (fee_new - fee_old)

# 7. Output Result
# Question asks for "amount delta".
# Positive delta means they pay MORE. Negative means they pay LESS.
# "what amount delta will it have to pay" -> usually implies the signed difference.
print(f"{total_delta:.14f}")

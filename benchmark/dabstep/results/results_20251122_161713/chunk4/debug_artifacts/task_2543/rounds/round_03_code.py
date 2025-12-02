# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2543
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 8706 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
        # Range handling (e.g., "50-60") - return mean
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

def parse_val(v):
    """Parse values like '100k', '1m', '8.3%' into floats for range comparison."""
    if isinstance(v, (int, float)): return float(v)
    if not isinstance(v, str): return 0.0
    v = v.lower().replace(',', '').strip()
    if 'k' in v:
        return float(v.replace('k', '')) * 1000
    if 'm' in v:
        return float(v.replace('m', '')) * 1000000
    if '%' in v:
        return float(v.replace('%', '')) / 100
    return float(v)

def check_range(rule_val, actual_val):
    """Check if actual_val falls within rule_val range (string or numeric)."""
    if rule_val is None: return True
    
    # If rule_val is a list (though usually ranges are strings), handle it
    if isinstance(rule_val, list):
        return actual_val in rule_val

    s_val = str(rule_val).strip()
    
    # Handle inequalities
    if s_val.startswith('>='):
        limit = parse_val(s_val[2:])
        return actual_val >= limit
    if s_val.startswith('>'):
        limit = parse_val(s_val[1:])
        return actual_val > limit
    if s_val.startswith('<='):
        limit = parse_val(s_val[2:])
        return actual_val <= limit
    if s_val.startswith('<'):
        limit = parse_val(s_val[1:])
        return actual_val < limit
        
    # Handle ranges 'min-max'
    if '-' in s_val:
        parts = s_val.split('-')
        if len(parts) == 2:
            try:
                min_v = parse_val(parts[0])
                max_v = parse_val(parts[1])
                return min_v <= actual_val <= max_v
            except:
                pass
            
    # Exact match (numeric)
    try:
        return actual_val == parse_val(s_val)
    except:
        return str(actual_val) == s_val

def check_capture_delay(rule_val, actual_val):
    """Match capture delay rules against merchant config."""
    if rule_val is None: return True
    
    # Normalize actual_val to number if possible for range checks
    # 'immediate' -> 0 days
    # 'manual' -> effectively infinite days for range checks, or specific category
    val_map = {'immediate': 0.0, 'manual': 9999.0}
    
    # 1. Exact string match
    if str(rule_val) == str(actual_val):
        return True
        
    # 2. Numeric/Range match
    try:
        # Get numeric representation of actual value
        if str(actual_val) in val_map:
            act_num = val_map[str(actual_val)]
        else:
            act_num = float(actual_val)
            
        return check_range(rule_val, act_num)
    except ValueError:
        return False

def is_not_empty(obj):
    """Check if list/array is not empty."""
    if obj is None: return False
    if isinstance(obj, list): return len(obj) > 0
    return False

# ==========================================
# MAIN LOGIC
# ==========================================

def solve():
    # 1. Load Data
    try:
        payments = pd.read_csv('/output/chunk4/data/context/payments.csv')
        with open('/output/chunk4/data/context/fees.json') as f:
            fees = json.load(f)
        with open('/output/chunk4/data/context/merchant_data.json') as f:
            merchant_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Filter for Merchant and Year
    merchant_name = 'Martinis_Fine_Steakhouse'
    # Filter for 2023
    df = payments[(payments['merchant'] == merchant_name) & (payments['year'] == 2023)].copy()
    
    if df.empty:
        print("0.00000000000000")
        return

    # 3. Get Merchant Config
    m_config = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
    if not m_config:
        print("Merchant config not found")
        return
    
    old_mcc = m_config['merchant_category_code']
    new_mcc = 8062
    account_type = m_config['account_type']
    capture_delay = m_config['capture_delay']
    
    # 4. Calculate Monthly Stats (Volume and Fraud Rate)
    # Map day_of_year to month for 2023 (non-leap year)
    cum_days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]
    
    def get_month(doy):
        for i in range(12):
            if doy <= cum_days[i+1]:
                return i + 1
        return 12

    df['month'] = df['day_of_year'].apply(get_month)
    
    monthly_stats = {}
    for m in range(1, 13):
        m_df = df[df['month'] == m]
        vol = m_df['eur_amount'].sum()
        fraud_vol = m_df[m_df['has_fraudulent_dispute']]['eur_amount'].sum()
        # Fraud rate = Fraud Volume / Total Volume
        # Note: Some definitions use count, but manual says "ratio of fraudulent volume over total volume" (Section 7)
        # Actually, Section 5 says "ratio between monthly total volume and monthly volume notified as fraud".
        # Let's stick to Volume/Volume as per manual.
        rate = fraud_vol / vol if vol > 0 else 0.0
        monthly_stats[m] = {'vol': vol, 'rate': rate}

    # 5. Fee Calculation Function
    def get_fee_for_tx(tx, mcc_code, m_stats):
        is_intra = (tx['issuing_country'] == tx['acquirer_country'])
        
        # Iterate through rules to find the FIRST match (assuming priority or specific order isn't strictly defined, 
        # but usually specific rules come first. The dataset doesn't specify priority, so we take first match 
        # that satisfies all conditions).
        # Note: In real engines, there's a priority score. Here we assume the JSON order is relevant or rules are mutually exclusive enough.
        
        for rule in fees:
            # 1. Scheme
            if rule['card_scheme'] != tx['card_scheme']: continue
            
            # 2. Account Type (Empty list [] means all)
            if is_not_empty(rule['account_type']) and account_type not in rule['account_type']: continue
            
            # 3. Capture Delay
            if not check_capture_delay(rule['capture_delay'], capture_delay): continue
            
            # 4. MCC (Empty list [] means all)
            if is_not_empty(rule['merchant_category_code']) and mcc_code not in rule['merchant_category_code']: continue
            
            # 5. Credit (Null means all)
            if rule['is_credit'] is not None and rule['is_credit'] != tx['is_credit']: continue
            
            # 6. ACI (Empty list [] means all)
            if is_not_empty(rule['aci']) and tx['aci'] not in rule['aci']: continue
            
            # 7. Intracountry (Null means all)
            if rule['intracountry'] is not None:
                # Handle 0.0/1.0/True/False
                rule_intra = bool(rule['intracountry'])
                if rule_intra != is_intra: continue
            
            # 8. Monthly Volume
            if not check_range(rule['monthly_volume'], m_stats['vol']): continue
            
            # 9. Monthly Fraud Level
            if not check_range(rule['monthly_fraud_level'], m_stats['rate']): continue
            
            # Match found
            return rule['fixed_amount'] + (rule['rate'] * tx['eur_amount'] / 10000)
            
        return 0.0

    # 6. Calculate Fees
    total_old = 0.0
    total_new = 0.0
    
    # Pre-calculate stats lookup to avoid dict lookup in loop
    # (Already done via monthly_stats dict)

    for _, tx in df.iterrows():
        m_stats = monthly_stats[tx['month']]
        
        fee_old = get_fee_for_tx(tx, old_mcc, m_stats)
        fee_new = get_fee_for_tx(tx, new_mcc, m_stats)
        
        total_old += fee_old
        total_new += fee_new

    # 7. Calculate Delta
    delta = total_new - total_old
    
    # Print with high precision
    print(f"{delta:.14f}")

if __name__ == "__main__":
    solve()

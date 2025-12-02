# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1780
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7118 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import re

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

def parse_range(range_str, value):
    """
    Checks if value is within range_str.
    Handles:
    - '100k-1m' (numeric ranges with suffixes)
    - '0%-0.5%' (percentage ranges)
    - '>5', '<3' (inequalities)
    - 'immediate', 'manual' (categorical matches)
    """
    if range_str is None:
        return True
    
    # Helper to parse "100k", "1m", "5%"
    def parse_val(s):
        s = str(s).lower().strip().replace('%', '')
        mult = 1
        if 'k' in s:
            mult = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mult = 1_000_000
            s = s.replace('m', '')
        try:
            return float(s) * mult
        except ValueError:
            return s # Return string if not number

    # Handle categorical exact matches first
    if isinstance(value, str) and not any(c in range_str for c in ['<', '>', '-']):
        return str(range_str).lower() == value.lower()

    # Detect if range implies percentage
    is_pct = '%' in str(range_str)
    
    try:
        if '-' in str(range_str):
            low_s, high_s = range_str.split('-')
            low = parse_val(low_s)
            high = parse_val(high_s)
            
            # If range was percentage (e.g. 7-8%), convert bounds to decimal if value is decimal
            # Assumption: value passed in is already decimal (e.g. 0.08)
            if is_pct:
                low /= 100.0
                high /= 100.0
                
            return low <= value <= high
            
        elif str(range_str).startswith('>'):
            limit = parse_val(range_str[1:])
            if is_pct: limit /= 100.0
            return value > limit
            
        elif str(range_str).startswith('<'):
            limit = parse_val(range_str[1:])
            if is_pct: limit /= 100.0
            return value < limit
            
        else:
            # Fallback for exact numeric match or string match
            return str(value) == str(range_str)
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_ctx must contain: mcc, account_type, capture_delay, monthly_volume, 
                       monthly_fraud_level, card_scheme, is_credit, aci, is_intracountry
    """
    # 1. Merchant Static Checks
    # Account Type (Rule is list, Merchant is single value)
    if rule['account_type'] and tx_ctx['account_type'] not in rule['account_type']:
        return False
    
    # MCC (Rule is list, Merchant is single value)
    if rule['merchant_category_code'] and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # Capture Delay
    if rule['capture_delay']:
        mdelay = str(tx_ctx['capture_delay'])
        
        # Map 'immediate' to 0 for numeric comparisons (e.g. <3)
        mdelay_val = 0.0 if mdelay == 'immediate' else mdelay
        try:
            mdelay_val = float(mdelay)
        except:
            pass # Keep as string/0.0
            
        if not parse_range(rule['capture_delay'], mdelay_val):
            # Double check string match if numeric failed (e.g. 'manual' vs 'manual')
            if mdelay != str(rule['capture_delay']):
                return False

    # 2. Monthly Stats Checks
    if rule['monthly_volume'] and not parse_range(rule['monthly_volume'], tx_ctx['monthly_volume']):
        return False
        
    if rule['monthly_fraud_level'] and not parse_range(rule['monthly_fraud_level'], tx_ctx['monthly_fraud_level']):
        return False

    # 3. Transaction Specific Checks
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    if rule['is_credit'] is not None and rule['is_credit'] != tx_ctx['is_credit']:
        return False
        
    if rule['aci'] and tx_ctx['aci'] not in rule['aci']:
        return False
        
    if rule['intracountry'] is not None:
        # JSON boolean/numbers: 0.0/0 -> False, 1.0/1 -> True
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['is_intracountry']:
            return False

    return True

# --- Main Execution ---

# 1. Load Data
df = pd.read_csv('/output/chunk6/data/context/payments.csv')
with open('/output/chunk6/data/context/merchant_data.json') as f:
    merchant_data = json.load(f)
with open('/output/chunk6/data/context/fees.json') as f:
    fees = json.load(f)

# 2. Define Context
merchant_name = 'Golfclub_Baron_Friso'
# June 2023: Day 152 to 181
start_day = 152
end_day = 181

# 3. Filter Data
df_june = df[
    (df['merchant'] == merchant_name) & 
    (df['day_of_year'] >= start_day) & 
    (df['day_of_year'] <= end_day) &
    (df['year'] == 2023)
].copy()

# 4. Get Merchant Static Data
m_info = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
if not m_info:
    print(f"Error: Merchant {merchant_name} not found in merchant_data.json")
    exit()

# 5. Calculate Monthly Stats
monthly_volume = df_june['eur_amount'].sum()
fraud_volume = df_june[df_june['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 6. Identify Applicable Fees
applicable_ids = set()

# Create derived column for intracountry check
df_june['is_intracountry'] = df_june['issuing_country'] == df_june['acquirer_country']

# Extract unique transaction profiles to optimize matching
# We only need to check each unique combination of fee-determining columns once
unique_txs = df_june[['card_scheme', 'is_credit', 'aci', 'is_intracountry']].drop_duplicates()

for _, tx in unique_txs.iterrows():
    # Build context for this transaction profile
    ctx = {
        'mcc': m_info['merchant_category_code'],
        'account_type': m_info['account_type'],
        'capture_delay': m_info['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level,
        'card_scheme': tx['card_scheme'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'is_intracountry': tx['is_intracountry']
    }
    
    # Check against all fee rules
    for rule in fees:
        if match_fee_rule(ctx, rule):
            applicable_ids.add(rule['ID'])

# 7. Output Results
sorted_ids = sorted(list(applicable_ids))
print(sorted_ids)

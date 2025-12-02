# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1731
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 6933 characters (FULL CODE)
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
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_range(range_str, is_percentage=False):
    """Parses range strings like '100k-1m', '<3%', '>5' into (min, max) tuples."""
    if range_str is None:
        return (-float('inf'), float('inf'))
    
    s = str(range_str).strip()
    
    def parse_val(v):
        v = v.lower().replace('%', '')
        mult = 1
        if 'k' in v:
            mult = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1000000
            v = v.replace('m', '')
        try:
            return float(v) * mult
        except:
            return 0.0

    scale = 0.01 if is_percentage else 1.0

    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]) * scale, parse_val(parts[1]) * scale)
    elif s.startswith('>'):
        return (parse_val(s[1:]) * scale, float('inf'))
    elif s.startswith('<'):
        return (-float('inf'), parse_val(s[1:]) * scale)
    else:
        # Exact match treated as point
        val = parse_val(s) * scale
        return (val, val)

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches the rule."""
    if rule_delay is None:
        return True
    
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    if m_delay == r_delay:
        return True
    
    # Handle numeric comparisons (e.g., merchant "1" vs rule "<3")
    if m_delay.isdigit():
        days = int(m_delay)
        if r_delay.startswith('<'):
            limit = int(r_delay[1:])
            return days < limit
        elif r_delay.startswith('>'):
            limit = int(r_delay[1:])
            return days > limit
        elif '-' in r_delay:
            low, high = map(int, r_delay.split('-'))
            return low <= days <= high
            
    return False

def calculate_fee(tx_amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = float(rule['fixed_amount'])
    rate = float(rule['rate'])
    return fixed + (rate * tx_amount / 10000.0)

# --- Main Execution ---

# File paths
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

# Load data
df = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)

# Target parameters
target_merchant = "Martinis_Fine_Steakhouse"
target_year = 2023
target_day = 100  # April 10th

# 1. Calculate Monthly Stats for April (Days 91-120)
# We need these to determine which fee tier applies
april_mask = (
    (df['merchant'] == target_merchant) &
    (df['year'] == target_year) &
    (df['day_of_year'] >= 91) &
    (df['day_of_year'] <= 120)
)
df_april = df[april_mask]

monthly_volume = df_april['eur_amount'].sum()
fraud_volume = df_april[df_april['has_fraudulent_dispute']]['eur_amount'].sum()
# Fraud level is ratio of fraud volume to total volume
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 2. Get Merchant Metadata
m_info = next((item for item in merchant_data if item['merchant'] == target_merchant), None)
if not m_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

merchant_stats = {
    'monthly_volume': monthly_volume,
    'monthly_fraud_rate': monthly_fraud_rate,
    'account_type': m_info.get('account_type'),
    'mcc': m_info.get('merchant_category_code'),
    'capture_delay': m_info.get('capture_delay')
}

# 3. Filter Transactions for the specific day (Day 100)
day_mask = (
    (df['merchant'] == target_merchant) &
    (df['year'] == target_year) &
    (df['day_of_year'] == target_day)
)
df_day = df[day_mask].copy()

# Calculate intracountry (True if issuing == acquirer)
df_day['intracountry'] = df_day['issuing_country'] == df_day['acquirer_country']

# 4. Calculate Fees for each transaction
total_fees = 0.0

# Debugging counters
matched_count = 0
unmatched_count = 0

for idx, tx in df_day.iterrows():
    tx_dict = tx.to_dict()
    
    matched_rule = None
    
    # Iterate through rules to find the first match
    for rule in fees:
        # 1. Card Scheme
        if rule['card_scheme'] != tx_dict['card_scheme']:
            continue
            
        # 2. Account Type (Wildcard: [])
        if rule['account_type'] and merchant_stats['account_type'] not in rule['account_type']:
            continue
            
        # 3. MCC (Wildcard: [])
        if rule['merchant_category_code'] and merchant_stats['mcc'] not in rule['merchant_category_code']:
            continue
            
        # 4. Is Credit (Wildcard: null)
        if rule['is_credit'] is not None:
            if rule['is_credit'] != tx_dict['is_credit']:
                continue
                
        # 5. ACI (Wildcard: [])
        if rule['aci'] and tx_dict['aci'] not in rule['aci']:
            continue
            
        # 6. Intracountry (Wildcard: null)
        if rule['intracountry'] is not None:
            is_intra = 1.0 if tx_dict['intracountry'] else 0.0
            if float(rule['intracountry']) != is_intra:
                continue
                
        # 7. Capture Delay (Wildcard: null)
        if not check_capture_delay(merchant_stats['capture_delay'], rule['capture_delay']):
            continue
            
        # 8. Monthly Volume (Wildcard: null)
        v_min, v_max = parse_range(rule['monthly_volume'], is_percentage=False)
        if not (v_min <= merchant_stats['monthly_volume'] <= v_max):
            continue
            
        # 9. Monthly Fraud Level (Wildcard: null)
        f_min, f_max = parse_range(rule['monthly_fraud_level'], is_percentage=True)
        if not (f_min <= merchant_stats['monthly_fraud_rate'] <= f_max):
            continue
            
        # Match found
        matched_rule = rule
        break
    
    if matched_rule:
        fee = calculate_fee(tx_dict['eur_amount'], matched_rule)
        total_fees += fee
        matched_count += 1
    else:
        unmatched_count += 1

# Output the result
print(f"{total_fees:.2f}")

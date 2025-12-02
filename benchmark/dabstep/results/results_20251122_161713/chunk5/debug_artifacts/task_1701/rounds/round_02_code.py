# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1701
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8637 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        
        # Handle percentages
        if '%' in v:
            return float(v.replace('%', '')) / 100
            
        # Handle k (thousands) and m (millions)
        lower_v = v.lower()
        if 'k' in lower_v:
            return float(lower_v.replace('k', '')) * 1_000
        if 'm' in lower_v:
            return float(lower_v.replace('m', '')) * 1_000_000
            
        # Range handling (e.g., "50-60") - return mean (fallback, though parse_range is preferred for rules)
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

def parse_range(range_str):
    """Parses strings like '100k-1m', '7.7%-8.3%', '>5', '<3' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Helper to parse individual values using coerce_to_float
    def parse_val(v):
        return coerce_to_float(v)

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif '>' in s:
        val = parse_val(s.replace('>', ''))
        return val, float('inf')
    elif '<' in s:
        val = parse_val(s.replace('<', ''))
        return float('-inf'), val
    else:
        # Exact match treated as range [val, val]
        val = parse_val(s)
        return val, val

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches rule."""
    if rule_delay is None:
        return True
    
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    if r_delay == m_delay:
        return True
    
    # Map merchant delay to days for numeric comparison
    # 'immediate' -> 0 days
    delay_map = {
        'immediate': 0,
        'manual': 999, # Treated as very high number for > comparisons
        '1': 1, '2': 2, '7': 7
    }
    
    m_days = delay_map.get(m_delay)
    if m_days is None:
        try:
            m_days = float(m_delay)
        except:
            return False # Unknown format
            
    # Handle rule ranges
    if '-' in r_delay:
        min_d, max_d = parse_range(r_delay)
        return min_d <= m_days <= max_d
    elif '>' in r_delay:
        val = coerce_to_float(r_delay)
        return m_days > val
    elif '<' in r_delay:
        val = coerce_to_float(r_delay)
        return m_days < val
    
    return False

def match_fee_rule(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    tx_context must contain:
    - card_scheme, account_type, merchant_category_code, capture_delay
    - monthly_volume, monthly_fraud_rate
    - is_credit, aci, intracountry
    """
    
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match - rule['account_type'] is a list)
    if rule.get('account_type'): # If list is not empty
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'): # If list is not empty
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Complex match)
    if rule.get('capture_delay'):
        if not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
            return False
            
    # 5. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_context['monthly_volume'] <= max_v):
            return False
            
    # 6. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        # Fraud rate in context is float (e.g. 0.08). Range parser handles %.
        if not (min_f <= tx_context['monthly_fraud_rate'] <= max_f):
            return False
            
    # 7. Is Credit (Bool match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 8. ACI (List match)
    if rule.get('aci'): # If list is not empty
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry (Bool match)
    if rule.get('intracountry') is not None:
        # rule['intracountry'] might be 0.0/1.0 or boolean in JSON
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False
            
    return True

# --- Main Execution ---

# Load files
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'

print("Loading data...")
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data_list = json.load(f)

# Target Parameters
target_merchant = 'Martinis_Fine_Steakhouse'
target_day = 100
target_year = 2023

# 1. Get Merchant Metadata
merchant_info = next((m for m in merchant_data_list if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Merchant {target_merchant} not found in merchant_data.json")
    exit()

print(f"Merchant Info: Account Type={merchant_info['account_type']}, MCC={merchant_info['merchant_category_code']}, Capture Delay={merchant_info['capture_delay']}")

# 2. Calculate Monthly Stats (April 2023)
# Day 100 falls in April.
# Jan (31) + Feb (28) + Mar (31) = 90 days.
# April starts Day 91, ends Day 120 (30 days).
april_start = 91
april_end = 120

april_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= april_start) &
    (df_payments['day_of_year'] <= april_end)
]

monthly_volume = april_txs['eur_amount'].sum()
fraud_volume = april_txs[april_txs['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

print(f"April Stats (Days {april_start}-{april_end}):")
print(f"  Volume: €{monthly_volume:,.2f}")
print(f"  Fraud Rate: {monthly_fraud_rate:.4%}")

# 3. Get Transactions for Day 100
day_100_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
]

print(f"Transactions on Day {target_day}: {len(day_100_txs)}")

if len(day_100_txs) == 0:
    print("No transactions found for this day.")
    exit()

# 4. Find Applicable Fees
applicable_fee_ids = set()

# Optimization: Get unique transaction profiles to avoid checking every single row
# Relevant columns for fee matching: card_scheme, is_credit, aci, issuing_country, acquirer_country
unique_profiles = day_100_txs[['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country']].drop_duplicates()

print(f"Unique transaction profiles to check: {len(unique_profiles)}")

for _, row in unique_profiles.iterrows():
    # Build context
    context = {
        'card_scheme': row['card_scheme'],
        'account_type': merchant_info['account_type'],
        'merchant_category_code': merchant_info['merchant_category_code'],
        'capture_delay': merchant_info['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate,
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['issuing_country'] == row['acquirer_country']
    }
    
    # Check against all rules
    for rule in fees_data:
        if match_fee_rule(context, rule):
            applicable_fee_ids.add(rule['ID'])

# 5. Output
sorted_ids = sorted(list(applicable_fee_ids))
print(f"Applicable Fee IDs: {', '.join(map(str, sorted_ids))}")

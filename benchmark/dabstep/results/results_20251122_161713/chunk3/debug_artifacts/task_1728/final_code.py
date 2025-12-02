import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v.lower():
            return float(v.lower().replace('k', '')) * 1000
        if 'm' in v.lower():
            return float(v.lower().replace('m', '')) * 1000000
        return float(v)
    return float(value)

def parse_range(range_str):
    """Parses a range string like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower()
    is_pct = '%' in s
    
    def parse_val(v):
        if 'k' in v: return float(v.replace('k', '').replace('%','')) * 1000
        if 'm' in v: return float(v.replace('m', '').replace('%','')) * 1000000
        val = float(v.replace('%', ''))
        return val / 100 if is_pct else val

    if '-' in s:
        parts = s.split('-')
        try:
            return parse_val(parts[0]), parse_val(parts[1])
        except:
            return None, None
    elif s.startswith('>'):
        return parse_val(s[1:]), float('inf')
    elif s.startswith('<'):
        return float('-inf'), parse_val(s[1:])
    else:
        try:
            v = parse_val(s)
            return v, v
        except:
            return None, None

def check_range(value, range_str):
    """Checks if a value falls within a range string."""
    if range_str is None:
        return True
    min_v, max_v = parse_range(range_str)
    if min_v is None: return True
    return min_v <= value <= max_v

def check_capture_delay(merchant_delay, rule_delay):
    """Matches merchant capture delay against rule."""
    if rule_delay is None:
        return True
    
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    if r_delay == m_delay:
        return True
        
    if m_delay in ['immediate', 'manual'] or r_delay in ['immediate', 'manual']:
        return m_delay == r_delay
        
    try:
        m_val = float(m_delay)
        min_v, max_v = parse_range(r_delay)
        if min_v is None: return False
        return min_v <= m_val <= max_v
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    """
    # 1. Card Scheme
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (Wildcard = [])
    if rule['account_type'] and tx_ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. MCC (Wildcard = [])
    if rule['merchant_category_code'] and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Is Credit (Wildcard = null)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_ctx['is_credit']:
        return False
        
    # 5. ACI (Wildcard = [])
    if rule['aci'] and tx_ctx['aci'] not in rule['aci']:
        return False
        
    # 6. Capture Delay (Wildcard = null)
    if not check_capture_delay(tx_ctx['capture_delay'], rule['capture_delay']):
        return False
        
    # 7. Monthly Volume (Wildcard = null)
    if not check_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
        return False
        
    # 8. Monthly Fraud Level (Wildcard = null)
    if not check_range(tx_ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
        return False
        
    # 9. Intracountry (Wildcard = null)
    if rule['intracountry'] is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False
            
    return True

def calculate_fee(amount, rule):
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# --- Main Execution ---

# Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data_list = json.load(f)

# Target Merchant
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023
target_day = 365

# 1. Get Merchant Metadata
merchant_info = next((m for m in merchant_data_list if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

m_account_type = merchant_info['account_type']
m_mcc = merchant_info['merchant_category_code']
m_capture_delay = merchant_info['capture_delay']

# 2. Calculate Monthly Stats (December 2023)
# December is days 335 to 365 (non-leap year)
dec_start = 335
dec_end = 365

df_dec = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= dec_start) &
    (df_payments['day_of_year'] <= dec_end)
]

monthly_volume = df_dec['eur_amount'].sum()

# Fraud Level: Fraud Volume / Total Volume
fraud_volume = df_dec[df_dec['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 3. Filter Target Transactions (Day 365)
df_target = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
]

# 4. Calculate Fees
total_fees = 0.0
matched_count = 0

for _, tx in df_target.iterrows():
    # Build Context
    is_intra = (tx['issuing_country'] == tx['acquirer_country'])
    
    ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': m_account_type,
        'mcc': m_mcc,
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'capture_delay': m_capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level,
        'intracountry': is_intra
    }
    
    # Find Rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(ctx, rule):
            matched_rule = rule
            break # Assume first match applies
            
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee
        matched_count += 1
    else:
        # Fallback or error logging if needed
        pass

print(f"{total_fees:.2f}")
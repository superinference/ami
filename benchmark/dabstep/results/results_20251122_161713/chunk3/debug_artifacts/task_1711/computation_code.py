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
    return 0.0

def parse_range(rule_str):
    """Parses strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into a check function."""
    if rule_str is None:
        return lambda x: True
    
    s = str(rule_str).strip().lower()
    if not s:
        return lambda x: True

    # Handle percentages
    is_percent = '%' in s
    clean_s = s.replace('%', '')
    
    def parse_val(v_str):
        v_str = v_str.strip()
        mult = 1
        if v_str.endswith('k'):
            mult = 1000
            v_str = v_str[:-1]
        elif v_str.endswith('m'):
            mult = 1000000
            v_str = v_str[:-1]
        try:
            val = float(v_str)
        except ValueError:
            return 0.0
        if is_percent:
            val = val / 100.0
        return val * mult

    try:
        if '-' in clean_s:
            parts = clean_s.split('-')
            low = parse_val(parts[0])
            high = parse_val(parts[1])
            return lambda x: low <= x <= high
        elif clean_s.startswith('>'):
            val = parse_val(clean_s[1:])
            return lambda x: x > val
        elif clean_s.startswith('<'):
            val = parse_val(clean_s[1:])
            return lambda x: x < val
        elif clean_s.startswith('≥') or clean_s.startswith('>='):
            val = parse_val(clean_s.replace('>=','').replace('≥',''))
            return lambda x: x >= val
        elif clean_s.startswith('≤') or clean_s.startswith('<='):
            val = parse_val(clean_s.replace('<=','').replace('≤',''))
            return lambda x: x <= val
        else:
            # Exact match
            val = parse_val(clean_s)
            return lambda x: x == val
    except Exception:
        return lambda x: False

def check_capture_delay(merchant_delay, rule_delay):
    if rule_delay is None:
        return True
    
    # Handle specific strings
    if str(rule_delay) in ['immediate', 'manual']:
        return str(merchant_delay) == str(rule_delay)
    
    # Handle numeric comparison
    try:
        m_val = float(merchant_delay)
        check_fn = parse_range(rule_delay)
        return check_fn(m_val)
    except ValueError:
        return False

def match_fee_rule(tx_context, rule):
    # 1. Card Scheme
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List in rule, single in merchant)
    if rule['account_type'] and tx_context['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List in rule, single in merchant)
    if rule['merchant_category_code'] and tx_context['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (Rule string/range, Merchant string)
    if not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
        return False
        
    # 5. Monthly Volume (Rule range, Calculated float)
    if rule['monthly_volume']:
        check_vol = parse_range(rule['monthly_volume'])
        if not check_vol(tx_context['monthly_volume']):
            return False
            
    # 6. Monthly Fraud Level (Rule range, Calculated float)
    if rule['monthly_fraud_level']:
        check_fraud = parse_range(rule['monthly_fraud_level'])
        if not check_fraud(tx_context['monthly_fraud_rate']):
            return False
            
    # 7. Is Credit (Rule bool, Tx bool)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_context['is_credit']:
        return False
        
    # 8. ACI (List in rule, single in tx)
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False
        
    # 9. Intracountry (Rule bool, Calculated bool)
    if rule['intracountry'] is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False
            
    return True

def calculate_fee(amount, rule):
    fixed = rule['fixed_amount']
    variable = (rule['rate'] * amount) / 10000
    return fixed + variable

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Setup Context
target_merchant = 'Belles_cookbook_store'
target_year = 2023
target_day = 10

# Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in metadata")

# 3. Calculate Monthly Stats (January 2023)
# Day 10 is in January. Jan is days 1-31.
jan_mask = (
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year) & 
    (df_payments['day_of_year'] >= 1) & 
    (df_payments['day_of_year'] <= 31)
)
df_jan = df_payments[jan_mask]

monthly_volume = df_jan['eur_amount'].sum()

# Fraud calculation: "ratio between monthly total volume and monthly volume notified as fraud"
# Interpretation: Fraud Volume / Total Volume
fraud_volume = df_jan[df_jan['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 4. Filter Target Transactions (Day 10)
day_mask = (
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year) & 
    (df_payments['day_of_year'] == target_day)
)
df_target = df_payments[day_mask]

# 5. Calculate Fees
total_fees = 0.0
matched_count = 0

for _, tx in df_target.iterrows():
    # Build context for matching
    tx_context = {
        'card_scheme': tx['card_scheme'],
        'account_type': merchant_info['account_type'],
        'mcc': merchant_info['merchant_category_code'],
        'capture_delay': merchant_info['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate,
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country']
    }
    
    # Find matching rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            matched_rule = rule
            break
    
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee
        matched_count += 1

# Output the final result
print(f"{total_fees:.14f}")
import pandas as pd
import json
import numpy as np

# --- HELPER FUNCTIONS ---
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v.lower():
            return float(v.lower().replace('k', '')) * 1000
        if 'm' in v.lower():
            return float(v.lower().replace('m', '')) * 1000000
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except:
            return None
    return None

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '>5%' into (min, max)."""
    if range_str is None:
        return (-float('inf'), float('inf'))
    
    s = str(range_str).strip().lower()
    
    # Handle percentages
    is_percent = '%' in s
    if is_percent:
        s = s.replace('%', '')
        scale = 0.01
    else:
        scale = 1.0
        
    # Handle k/m suffixes for volume
    def parse_val(val_s):
        val_s = val_s.strip()
        mult = 1
        if 'k' in val_s:
            mult = 1000
            val_s = val_s.replace('k', '')
        elif 'm' in val_s:
            mult = 1000000
            val_s = val_s.replace('m', '')
        try:
            return float(val_s) * mult * scale
        except ValueError:
            return 0.0

    try:
        if '-' in s:
            parts = s.split('-')
            return (parse_val(parts[0]), parse_val(parts[1]))
        elif s.startswith('>'):
            return (parse_val(s[1:]), float('inf'))
        elif s.startswith('<'):
            return (-float('inf'), parse_val(s[1:]))
        else:
            # Exact value treated as range [val, val]
            v = parse_val(s)
            return (v, v)
    except:
        return (-float('inf'), float('inf'))

def match_fee_rule(tx_context, rule):
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List in rule, single in merchant)
    # Rule: [] or None matches all. Else merchant's type must be in list.
    if rule.get('account_type') and len(rule['account_type']) > 0:
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List in rule, single in merchant)
    if rule.get('merchant_category_code') and len(rule['merchant_category_code']) > 0:
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (String in rule, string in merchant)
    # Rule: None matches all. Else exact match.
    if rule.get('capture_delay'):
        if str(rule['capture_delay']) != str(tx_context['capture_delay']):
             return False

    # 5. Is Credit (Bool)
    # Rule: None matches all.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 6. ACI (List in rule, single in tx)
    if rule.get('aci') and len(rule['aci']) > 0:
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 7. Intracountry (Bool)
    # Rule: None matches all.
    if rule.get('intracountry') is not None:
        # Convert 0.0/1.0 to bool if needed
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['is_intracountry']:
            return False
            
    # 8. Monthly Volume (Range string in rule)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_context['monthly_volume'] <= max_v):
            return False
            
    # 9. Monthly Fraud Level (Range string in rule)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if not (min_f <= tx_context['monthly_fraud_ratio'] <= max_f):
            return False
            
    return True

# --- MAIN LOGIC ---

# 1. Load Data
base_path = '/output/chunk6/data/context/'
df_payments = pd.read_csv(base_path + 'payments.csv')
with open(base_path + 'merchant_data.json', 'r') as f:
    merchant_data = json.load(f)
with open(base_path + 'fees.json', 'r') as f:
    fees_data = json.load(f)

# 2. Get Merchant Attributes
target_merchant = 'Crossfit_Hanna'
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print("Merchant not found")
    exit()

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 3. Calculate Monthly Stats for Jan 2023
# Filter for Jan 2023 (Day 1-31)
jan_mask = (df_payments['merchant'] == target_merchant) & \
           (df_payments['year'] == 2023) & \
           (df_payments['day_of_year'] >= 1) & \
           (df_payments['day_of_year'] <= 31)
df_jan = df_payments[jan_mask]

total_volume = df_jan['eur_amount'].sum()
fraud_volume = df_jan[df_jan['has_fraudulent_dispute'] == True]['eur_amount'].sum()
fraud_ratio = fraud_volume / total_volume if total_volume > 0 else 0.0

# 4. Filter for Day 10 Transactions
day10_mask = (df_payments['merchant'] == target_merchant) & \
             (df_payments['year'] == 2023) & \
             (df_payments['day_of_year'] == 10)
df_day10 = df_payments[day10_mask]

# 5. Find Applicable Fees
applicable_ids = set()

for _, tx in df_day10.iterrows():
    # Build transaction context
    is_intra = (tx['issuing_country'] == tx['acquirer_country'])
    
    ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'mcc': mcc,
        'capture_delay': capture_delay,
        'is_credit': bool(tx['is_credit']),
        'aci': tx['aci'],
        'is_intracountry': is_intra,
        'monthly_volume': total_volume,
        'monthly_fraud_ratio': fraud_ratio
    }
    
    # Check against all fees
    for rule in fees_data:
        if match_fee_rule(ctx, rule):
            applicable_ids.add(rule['ID'])

# 6. Output
sorted_ids = sorted(list(applicable_ids))
print(", ".join(map(str, sorted_ids)))
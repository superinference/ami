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
        try:
            return float(v)
        except:
            return 0.0
    return float(value) if value is not None else 0.0

def is_not_empty(array):
    """Check if array/list is not empty. Handles numpy arrays safely."""
    if array is None:
        return False
    if hasattr(array, 'size'):  # numpy array
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

def parse_range_bounds(range_str):
    """Parses range strings like '100k-1m', '>5', '<3' into (min, max)."""
    if range_str is None:
        return (-float('inf'), float('inf'))
    
    s = str(range_str).strip().lower()
    s = s.replace(',', '').replace('%', '')
    
    def parse_val(v):
        mult = 1
        if 'k' in v: mult = 1000; v = v.replace('k', '')
        elif 'm' in v: mult = 1000000; v = v.replace('m', '')
        try:
            return float(v) * mult
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif s.startswith('>'):
        return (parse_val(s[1:]), float('inf'))
    elif s.startswith('<'):
        return (-float('inf'), parse_val(s[1:]))
    else:
        val = parse_val(s)
        return (val, val)

def check_range_match(value, range_str, is_percentage=False):
    """Checks if a value falls within a range string."""
    if range_str is None: return True
    
    # If checking percentage, value 0.08 (8%) should match "8%" (parsed as 8)
    # So we scale value by 100
    check_val = value * 100 if is_percentage else value
    
    low, high = parse_range_bounds(range_str)
    
    s = str(range_str).strip()
    if s.startswith('>'): return check_val > low
    if s.startswith('<'): return check_val < high
    
    return low <= check_val <= high

def match_fee_rule(tx_ctx, rule):
    """Matches a transaction context against a fee rule."""
    # 1. Card Scheme
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (list in rule)
    if is_not_empty(rule['account_type']):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. MCC (list in rule)
    if is_not_empty(rule['merchant_category_code']):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. is_credit (bool or null)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 5. ACI (list in rule)
    if is_not_empty(rule['aci']):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Capture Delay
    if rule['capture_delay'] is not None:
        m_val = str(tx_ctx['capture_delay'])
        r_val = str(rule['capture_delay'])
        
        if m_val != r_val:
            # Try numeric comparison if merchant value is digit
            if m_val.isdigit():
                d = int(m_val)
                if r_val.startswith('>'):
                    if not d > float(r_val[1:]): return False
                elif r_val.startswith('<'):
                    if not d < float(r_val[1:]): return False
                elif '-' in r_val:
                    low, high = parse_range_bounds(r_val)
                    if not (low <= d <= high): return False
                else:
                    return False
            else:
                return False
    
    # 7. Monthly Volume (range)
    if rule['monthly_volume'] is not None:
        if not check_range_match(tx_ctx['monthly_volume'], rule['monthly_volume'], is_percentage=False):
            return False
            
    # 8. Monthly Fraud Level (range)
    if rule['monthly_fraud_level'] is not None:
        if not check_range_match(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level'], is_percentage=True):
            return False
            
    # 9. Intracountry (bool)
    if rule['intracountry'] is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False
            
    return True

# --- Main Execution ---

# Define file paths
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

# 1. Load Data
print("Loading data...")
df_payments = pd.read_csv(payments_path)

with open(merchant_data_path, 'r') as f:
    merchant_data_list = json.load(f)

with open(fees_path, 'r') as f:
    fees_list = json.load(f)

# 2. Define Context
merchant_name = 'Golfclub_Baron_Friso'
start_day = 121
end_day = 151

# 3. Calculate Merchant Stats for May (Required for Fee Rules)
# We need the stats for the WHOLE month for this merchant to determine the applicable fee tier
may_txs = df_payments[
    (df_payments['merchant'] == merchant_name) &
    (df_payments['day_of_year'] >= start_day) &
    (df_payments['day_of_year'] <= end_day)
]

total_volume = may_txs['eur_amount'].sum()
fraud_volume = may_txs[may_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
# Fraud rate defined as Fraud Volume / Total Volume
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

print(f"Merchant: {merchant_name}")
print(f"May Total Volume: €{total_volume:,.2f}")
print(f"May Fraud Rate: {fraud_rate:.4%}")

# 4. Get Merchant Static Data
merchant_info = next((item for item in merchant_data_list if item["merchant"] == merchant_name), None)
if not merchant_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

mcc = merchant_info.get('merchant_category_code')
account_type = merchant_info.get('account_type')
capture_delay = merchant_info.get('capture_delay')

# 5. Filter Target Transactions (Fraudulent ones to analyze)
target_txs = may_txs[may_txs['has_fraudulent_dispute'] == True].copy()
print(f"Analyzing {len(target_txs)} fraudulent transactions for fee optimization...")

# 6. Simulate Fees for each ACI
aci_options = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
results = {}

for aci in aci_options:
    total_fee_for_aci = 0.0
    
    for _, tx in target_txs.iterrows():
        # Construct transaction context with the SIMULATED ACI
        ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': account_type,
            'mcc': mcc,
            'is_credit': bool(tx['is_credit']),
            'aci': aci,  # <--- This is the variable we are testing
            'monthly_volume': total_volume,
            'monthly_fraud_rate': fraud_rate,
            'capture_delay': capture_delay,
            'intracountry': tx['issuing_country'] == tx['acquirer_country']
        }
        
        # Find the first matching fee rule
        matched_rule = None
        for rule in fees_list:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            # Fee = Fixed + (Rate * Amount / 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000)
            total_fee_for_aci += fee
        else:
            # If no rule matches, we assume 0 or skip (should not happen with complete rules)
            pass
            
    results[aci] = total_fee_for_aci
    print(f"Total Fees for ACI '{aci}': €{total_fee_for_aci:.2f}")

# 7. Determine Preferred Choice
best_aci = min(results, key=results.get)
print(f"\nPreferred ACI (Lowest Fees): {best_aci}")
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
        return float(v)
    return float(value)

def parse_volume_range(range_str, value):
    """Check if value falls within the volume range string (e.g., '100k-1m')."""
    if range_str is None:
        return True
    
    # Normalize value to simple float
    val = float(value)
    
    s = range_str.lower().replace(',', '').strip()
    
    # Handle > and <
    is_gt = s.startswith('>')
    is_lt = s.startswith('<')
    s = s.lstrip('><')
    
    def parse_num(n_str):
        m = 1
        if 'k' in n_str:
            m = 1000
            n_str = n_str.replace('k', '')
        elif 'm' in n_str:
            m = 1000000
            n_str = n_str.replace('m', '')
        return float(n_str) * m

    if '-' in s:
        parts = s.split('-')
        try:
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            return low <= val <= high
        except:
            return False
    
    try:
        limit = parse_num(s)
        if is_gt:
            return val > limit
        if is_lt:
            return val < limit
        return val == limit
    except:
        return False

def parse_fraud_range(range_str, value):
    """Check if value (0.0-1.0) falls within fraud range string (e.g., '0.0%-0.5%')."""
    if range_str is None:
        return True
    
    s = range_str.replace('%', '').strip()
    
    # Handle > and <
    is_gt = s.startswith('>')
    is_lt = s.startswith('<')
    s = s.lstrip('><')
    
    def parse_pct(n_str):
        return float(n_str) / 100.0

    if '-' in s:
        parts = s.split('-')
        try:
            low = parse_pct(parts[0])
            high = parse_pct(parts[1])
            return low <= value <= high
        except:
            return False
    
    try:
        limit = parse_pct(s)
        if is_gt:
            return value > limit
        if is_lt:
            return value < limit
        return False
    except:
        return False

def match_fee_rule(tx_data, merchant_data, monthly_vol, monthly_fraud, rule):
    """
    Determines if a fee rule applies to a specific transaction.
    """
    
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_data['card_scheme']:
        return False
        
    # 2. Account Type (List contains)
    if rule['account_type'] and merchant_data['account_type'] not in rule['account_type']:
        return False
        
    # 3. Capture Delay (Exact match or wildcard)
    if rule['capture_delay'] is not None:
        if str(rule['capture_delay']) != str(merchant_data['capture_delay']):
            return False
            
    # 4. Merchant Category Code (List contains)
    if rule['merchant_category_code'] and merchant_data['merchant_category_code'] not in rule['merchant_category_code']:
        return False
        
    # 5. Is Credit (Exact match or wildcard)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_data['is_credit']:
            return False
            
    # 6. ACI (List contains or wildcard)
    if rule['aci'] and tx_data['aci'] not in rule['aci']:
        return False
        
    # 7. Intracountry (Boolean match or wildcard)
    # Intracountry = (Issuing Country == Acquirer Country)
    is_intra = (tx_data['issuing_country'] == tx_data['acquirer_country'])
    if rule['intracountry'] is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False
            
    # 8. Monthly Volume (Range check)
    if not parse_volume_range(rule['monthly_volume'], monthly_vol):
        return False
        
    # 9. Monthly Fraud Level (Range check)
    if not parse_fraud_range(rule['monthly_fraud_level'], monthly_fraud):
        return False
        
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    # fee = fixed_amount + rate * transaction_value / 10000
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000.0)

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_list = json.load(f)

# 2. Filter for Martinis_Fine_Steakhouse in May 2023
target_merchant = 'Martinis_Fine_Steakhouse'
target_year = 2023
# May is roughly day 121 to 151 (non-leap year)
start_day = 121
end_day = 151

df_merchant = df[df['merchant'] == target_merchant]
df_may = df_merchant[
    (df_merchant['year'] == target_year) & 
    (df_merchant['day_of_year'] >= start_day) & 
    (df_merchant['day_of_year'] <= end_day)
].copy()

# 3. Get Merchant Attributes
merchant_info = next((item for item in merchant_list if item["merchant"] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

# 4. Calculate Monthly Stats (Volume and Fraud) for May
monthly_volume = df_may['eur_amount'].sum()
monthly_fraud_count = df_may['has_fraudulent_dispute'].sum()
total_tx_count = len(df_may)
monthly_fraud_rate = monthly_fraud_count / total_tx_count if total_tx_count > 0 else 0.0

# 5. Calculate Fees
total_fees = 0.0
unmatched_count = 0

for index, row in df_may.iterrows():
    tx_data = row.to_dict()
    
    matched_rule = None
    
    # Iterate through fees to find the first matching rule
    for rule in fees_data:
        if match_fee_rule(tx_data, merchant_info, monthly_volume, monthly_fraud_rate, rule):
            matched_rule = rule
            break
    
    if matched_rule:
        fee = calculate_fee(tx_data['eur_amount'], matched_rule)
        total_fees += fee
    else:
        unmatched_count += 1

# Output the result
print(f"{total_fees:.2f}")
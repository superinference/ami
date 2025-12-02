import pandas as pd
import json
import datetime
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

def parse_range_check(value, rule_range_str):
    """
    Checks if a numeric value falls within a range string (e.g., '100k-1m', '>5', '7.7%-8.3%').
    Returns True if match or if rule_range_str is None/Empty.
    """
    if not rule_range_str:
        return True
    
    try:
        s = str(rule_range_str).strip().lower()
        
        # Handle percentages
        is_percent = '%' in s
        
        # Helper to parse single number with k/m suffix
        def parse_num(n_str):
            n_str = n_str.replace('%', '')
            mult = 1
            if 'k' in n_str:
                mult = 1000
                n_str = n_str.replace('k', '')
            elif 'm' in n_str:
                mult = 1000000
                n_str = n_str.replace('m', '')
            return float(n_str) * mult

        if '-' in s:
            parts = s.split('-')
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            if is_percent:
                low /= 100
                high /= 100
            return low <= value <= high
            
        if s.startswith('>'):
            limit = parse_num(s[1:])
            if is_percent: limit /= 100
            return value > limit
            
        if s.startswith('<'):
            limit = parse_num(s[1:])
            if is_percent: limit /= 100
            return value < limit
            
        # Exact match (rare for ranges, but possible)
        val = parse_num(s)
        if is_percent: val /= 100
        return value == val
        
    except Exception as e:
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    """
    # 1. Card Scheme (Exact match required)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match or wildcard)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match or wildcard)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Exact match or wildcard)
    if rule.get('capture_delay'):
        if str(rule['capture_delay']) != str(tx_context['capture_delay']):
            return False
            
    # 5. Is Credit (Exact match or wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 6. ACI (List match or wildcard)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 7. Intracountry (Bool match or wildcard)
    if rule.get('intracountry') is not None:
        if rule['intracountry'] != tx_context['intracountry']:
            return False
            
    # 8. Monthly Volume (Range match or wildcard)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 9. Monthly Fraud Level (Range match or wildcard)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False
            
    return True

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
merchant_path = '/output/chunk4/data/context/merchant_data.json'
fees_path = '/output/chunk4/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Define Context
target_merchant = 'Belles_cookbook_store'
target_year = 2023
target_day_of_year = 300

# 3. Get Merchant Static Attributes
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_info:
    print(f"Merchant {target_merchant} not found in merchant_data.json")
    exit()

m_account_type = merchant_info.get('account_type')
m_mcc = merchant_info.get('merchant_category_code')
m_capture_delay = merchant_info.get('capture_delay')

# 4. Calculate Monthly Stats
# Determine month of day 300
date_obj = datetime.datetime(target_year, 1, 1) + datetime.timedelta(days=target_day_of_year - 1)
target_month = date_obj.month

# Filter for merchant
df_merchant_all = df_payments[df_payments['merchant'] == target_merchant].copy()

# Add date column to merchant data
df_merchant_all['date'] = pd.to_datetime(df_merchant_all['year'] * 1000 + df_merchant_all['day_of_year'], format='%Y%j')
df_merchant_all['month'] = df_merchant_all['date'].dt.month

# Filter for target month (October)
df_month = df_merchant_all[df_merchant_all['month'] == target_month]

# Calculate stats
monthly_volume = df_month['eur_amount'].sum()
monthly_fraud_count = df_month['has_fraudulent_dispute'].sum()
monthly_tx_count = len(df_month)
monthly_fraud_rate = (monthly_fraud_count / monthly_tx_count) if monthly_tx_count > 0 else 0.0

print(f"Merchant: {target_merchant}")
print(f"Month: {target_month}")
print(f"Monthly Volume: {monthly_volume}")
print(f"Monthly Fraud Rate: {monthly_fraud_rate}")

# 5. Filter Target Transactions (Day 300)
df_target_day = df_merchant_all[df_merchant_all['day_of_year'] == target_day_of_year]

print(f"Transactions on Day {target_day_of_year}: {len(df_target_day)}")

# 6. Match Fees
applicable_fee_ids = set()

for index, row in df_target_day.iterrows():
    # Build context for this transaction
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
    tx_context = {
        'card_scheme': row['card_scheme'],
        'account_type': m_account_type,
        'mcc': m_mcc,
        'capture_delay': m_capture_delay,
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': is_intracountry,
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Check all rules
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            applicable_fee_ids.add(rule['ID'])

# 7. Output
sorted_ids = sorted(list(applicable_fee_ids))
print(f"Applicable Fee IDs: {sorted_ids}")
print(", ".join(map(str, sorted_ids)))
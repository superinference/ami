import pandas as pd
import json
import datetime
import numpy as np

# Helper functions for robust data processing
def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float. Handles common formats."""
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
        if 'k' in v.lower():
            try:
                return float(v.lower().replace('k', '')) * 1000
            except ValueError:
                return 0.0
        if 'm' in v.lower():
            try:
                return float(v.lower().replace('m', '')) * 1000000
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

def parse_range(range_str):
    """Parses a range string like '100k-1m', '<3', '>5' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip()
    if s.startswith('<'):
        return 0, coerce_to_float(s[1:])
    if s.startswith('>'):
        return coerce_to_float(s[1:]), float('inf')
    
    if '-' in s:
        parts = s.split('-')
        # Handle cases like "7.7%-8.3%"
        return coerce_to_float(parts[0]), coerce_to_float(parts[1])
    
    return None, None

def check_numeric_rule(value, rule_value):
    """Checks if a numeric value fits the rule (range or null)."""
    if rule_value is None:
        return True
    min_v, max_v = parse_range(rule_value)
    if min_v is None and max_v is None:
        # Fallback for non-range strings if any
        return str(value) == str(rule_value)
    return min_v <= value <= max_v

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List contains value, or wildcard)
    if is_not_empty(rule.get('account_type')):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List contains value, or wildcard)
    if is_not_empty(rule.get('merchant_category_code')):
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Boolean match, or wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 5. ACI (List contains value, or wildcard)
    if is_not_empty(rule.get('aci')):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean match, or wildcard)
    if rule.get('intracountry') is not None:
        # Convert rule string/float to bool
        # 0.0 -> False, 1.0 -> True
        try:
            rule_intra = bool(float(rule['intracountry']))
            if rule_intra != tx_ctx['intracountry']:
                return False
        except ValueError:
            pass

    # 7. Capture Delay (String match/Range, or wildcard)
    if rule.get('capture_delay') is not None:
        rd = rule['capture_delay']
        md = str(tx_ctx['capture_delay'])
        
        # Handle categorical exact matches
        if rd in ['manual', 'immediate'] or md in ['manual', 'immediate']:
             if md != rd:
                 return False
        else:
            # Numeric comparison
            try:
                days = float(md)
                min_d, max_d = parse_range(rd)
                if min_d is not None:
                    if not (min_d <= days <= max_d):
                        return False
            except:
                return False

    # 8. Monthly Volume (Range check)
    if rule.get('monthly_volume') is not None:
        if not check_numeric_rule(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level') is not None:
        if not check_numeric_rule(tx_ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False
            
    return True

# Define file paths
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'
fees_path = '/output/chunk2/data/context/fees.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_rules = json.load(f)

# 2. Setup Target Context
target_merchant = 'Rafa_AI'
target_year = 2023
target_day_of_year = 200

# Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 3. Calculate Monthly Stats (Volume & Fraud)
# Determine month for day 200
date_obj = datetime.datetime(target_year, 1, 1) + datetime.timedelta(days=target_day_of_year - 1)
target_month = date_obj.month

# Calculate start and end day_of_year for the month to filter efficiently
start_date = datetime.datetime(target_year, target_month, 1)
if target_month == 12:
    end_date = datetime.datetime(target_year + 1, 1, 1) - datetime.timedelta(days=1)
else:
    end_date = datetime.datetime(target_year, target_month + 1, 1) - datetime.timedelta(days=1)

start_doy = start_date.timetuple().tm_yday
end_doy = end_date.timetuple().tm_yday

# Filter for the whole month of July 2023 for this merchant
df_month = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year) & 
    (df_payments['day_of_year'] >= start_doy) &
    (df_payments['day_of_year'] <= end_doy)
]

# Calculate Monthly Volume
monthly_volume = df_month['eur_amount'].sum()

# Calculate Monthly Fraud Level (Volume based per Manual Section 7)
fraud_volume = df_month[df_month['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_level = (fraud_volume / monthly_volume) if monthly_volume > 0 else 0.0

# 4. Filter Transactions for the Specific Day (200)
df_day = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year) & 
    (df_payments['day_of_year'] == target_day_of_year)
]

# 5. Calculate Fees
total_fees = 0.0

for _, tx in df_day.iterrows():
    # Build Context
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': merchant_info['account_type'],
        'merchant_category_code': merchant_info['merchant_category_code'],
        'is_credit': bool(tx['is_credit']),
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country'],
        'capture_delay': merchant_info['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    # Find Matching Rule
    matched_rule = None
    for rule in fees_rules:
        if match_fee_rule(tx_ctx, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Calculate Fee: fixed + (rate * amount / 10000)
        fixed = matched_rule['fixed_amount']
        rate = matched_rule['rate']
        amount = tx['eur_amount']
        
        fee = fixed + (rate * amount / 10000)
        total_fees += fee

# 6. Output Result
print(f"{total_fees:.2f}")
# Helper functions for robust data processing
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

def safe_get(df, column, default=None):
    """Safely get column from DataFrame, return default if not exists."""
    if isinstance(df, dict):
        return df.get(column, default)
    elif hasattr(df, 'columns') and column in df.columns:
        return df[column]
    return default

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

def safe_index(array, idx, default=None):
    """Safely get array element at index, return default if out of bounds."""
    try:
        if 0 <= idx < len(array):
            return array[idx]
        return default
    except (IndexError, TypeError, AttributeError):
        return default


import pandas as pd
import json
import numpy as np

def parse_range(range_str):
    """Parses range strings like '100k-1m', '<3', '>5', '7.7%-8.3%'."""
    if range_str is None:
        return None
    
    # Handle percentages
    is_percent = '%' in range_str
    clean_str = range_str.replace('%', '').replace(',', '')
    
    # Handle k/m suffixes
    def parse_val(v):
        if 'k' in v: return float(v.replace('k', '')) * 1000
        if 'm' in v: return float(v.replace('m', '')) * 1000000
        return float(v)

    if '-' in clean_str:
        low, high = clean_str.split('-')
        low = parse_val(low)
        high = parse_val(high)
        if is_percent:
            low /= 100
            high /= 100
        return (low, high)
    elif '<' in clean_str:
        val = parse_val(clean_str.replace('<', ''))
        if is_percent: val /= 100
        return (float('-inf'), val)
    elif '>' in clean_str:
        val = parse_val(clean_str.replace('>', ''))
        if is_percent: val /= 100
        return (val, float('inf'))
    return None

def match_fee_rule(tx, rule, merchant_attrs, monthly_stats):
    # 1. Card Scheme
    if rule['card_scheme'] != tx['card_scheme']:
        return False
        
    # 2. Account Type (List in rule, single value in merchant)
    if rule['account_type'] and merchant_attrs['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List in rule, single value in merchant)
    if rule['merchant_category_code'] and merchant_attrs['merchant_category_code'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (String match or wildcard)
    if rule['capture_delay'] is not None:
        # Handle inequality strings in rule vs exact value in merchant
        # For simplicity in this dataset, usually exact match or wildcard, 
        # but let's check if rule is a range/inequality vs a static string.
        # Given the context, simple string match or logic is usually sufficient.
        # If rule is '>5' and merchant is '7', that's a match.
        r_cd = rule['capture_delay']
        m_cd = merchant_attrs['capture_delay']
        
        if r_cd == 'immediate' or r_cd == 'manual':
            if r_cd != m_cd: return False
        elif '>' in r_cd or '<' in r_cd or '-' in r_cd:
            # Parse numeric delay
            try:
                m_val = float(m_cd)
                rng = parse_range(r_cd)
                if not (rng[0] <= m_val <= rng[1]):
                    return False
            except:
                # If merchant delay is not numeric (e.g. 'immediate') but rule is numeric
                return False
        else:
            if r_cd != m_cd: return False

    # 5. Is Credit
    if rule['is_credit'] is not None and rule['is_credit'] != tx['is_credit']:
        return False
        
    # 6. ACI (List in rule)
    if rule['aci'] and tx['aci'] not in rule['aci']:
        return False
        
    # 7. Intracountry
    is_intra = (tx['issuing_country'] == tx['acquirer_country'])
    if rule['intracountry'] is not None:
        # rule['intracountry'] is likely 0.0 or 1.0 or boolean
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False
            
    # 8. Monthly Volume
    if rule['monthly_volume']:
        rng = parse_range(rule['monthly_volume'])
        if not (rng[0] <= monthly_stats['volume'] <= rng[1]):
            return False
            
    # 9. Monthly Fraud Level
    if rule['monthly_fraud_level']:
        rng = parse_range(rule['monthly_fraud_level'])
        # Fraud level is ratio (0.0 - 1.0)
        if not (rng[0] <= monthly_stats['fraud_rate'] <= rng[1]):
            return False
            
    return True

# Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_path = '/output/chunk6/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data_list = json.load(f)

# Target Merchant and Date
target_merchant = "Martinis_Fine_Steakhouse"
target_year = 2023
target_day = 200

# Get Merchant Attributes
merchant_attrs = next((m for m in merchant_data_list if m['merchant'] == target_merchant), None)
if not merchant_attrs:
    print(f"Merchant {target_merchant} not found.")
    exit()

# Calculate Monthly Stats (July 2023)
# Day 200 is July 19th, 2023. So we need stats for July.
# July starts day 182, ends day 212 (approx, let's use datetime to be precise)
df_payments['date'] = pd.to_datetime(df_payments['year'] * 1000 + df_payments['day_of_year'], format='%Y%j')
df_payments['month'] = df_payments['date'].dt.month

# Filter for Merchant and Month 7 (July)
df_month = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year) & 
    (df_payments['month'] == 7)
]

monthly_volume = df_month['eur_amount'].sum()
fraud_txs = df_month[df_month['has_fraudulent_dispute']]
fraud_vol = fraud_txs['eur_amount'].sum()
monthly_fraud_rate = (fraud_vol / monthly_volume) if monthly_volume > 0 else 0

monthly_stats = {
    'volume': monthly_volume,
    'fraud_rate': monthly_fraud_rate
}

# Filter for Specific Day (200)
df_day = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year) & 
    (df_payments['day_of_year'] == target_day)
]

# Find Applicable Fee IDs
applicable_fee_ids = set()

# Iterate through unique transaction patterns to save time, or just all rows
# Relevant columns for fee matching: card_scheme, is_credit, aci, issuing_country, acquirer_country
# Note: eur_amount is used for fee calculation, not rule matching (except via monthly volume which is constant for the batch)

for _, tx in df_day.iterrows():
    for rule in fees_data:
        if match_fee_rule(tx, rule, merchant_attrs, monthly_stats):
            applicable_fee_ids.add(rule['ID'])

# Output
print(f"Applicable Fee IDs for {target_merchant} on Day {target_day}, {target_year}:")
print(sorted(list(applicable_fee_ids)))
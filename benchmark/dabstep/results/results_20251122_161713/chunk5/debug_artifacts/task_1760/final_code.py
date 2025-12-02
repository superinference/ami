import pandas as pd
import json
import re

# --- Helper Functions ---

def coerce_to_float(value):
    """
    Robustly converts strings with units (%, k, m), currency, or commas to float.
    Examples: '8.3%' -> 0.083, '100k' -> 100000, '€500' -> 500.0
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    
    s = str(value).strip().lower()
    s = s.replace(',', '').replace('€', '').replace('$', '')
    s = s.lstrip('><≤≥') # Remove inequality operators for pure number conversion
    
    try:
        if '%' in s:
            return float(s.replace('%', '')) / 100.0
        
        multiplier = 1
        if 'k' in s:
            multiplier = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            multiplier = 1000000
            s = s.replace('m', '')
            
        return float(s) * multiplier
    except (ValueError, TypeError):
        return None

def parse_range(rule_val, actual_val):
    """
    Checks if actual_val satisfies the rule_val condition.
    rule_val: "100k-1m", ">5", "manual", "0.0%-0.8%", etc.
    actual_val: The calculated metric or attribute.
    """
    if rule_val is None:
        return True
        
    # 1. Exact String Match (Case-insensitive)
    str_rule = str(rule_val).lower().strip()
    str_actual = str(actual_val).lower().strip()
    if str_rule == str_actual:
        return True

    # 2. Numeric/Range Logic
    # Convert actual value to float
    ac_float = coerce_to_float(actual_val)
    if ac_float is None:
        return False # Cannot compare non-numeric actual against numeric rule

    # Handle Range "min-max"
    if '-' in str_rule:
        parts = str_rule.split('-')
        if len(parts) == 2:
            min_v = coerce_to_float(parts[0])
            max_v = coerce_to_float(parts[1])
            if min_v is not None and max_v is not None:
                return min_v <= ac_float <= max_v

    # Handle Inequality ">val" or "<val"
    if str_rule.startswith('>'):
        limit = coerce_to_float(str_rule[1:])
        if limit is not None:
            return ac_float > limit
    if str_rule.startswith('<'):
        limit = coerce_to_float(str_rule[1:])
        if limit is not None:
            return ac_float < limit

    # Handle Exact Numeric Match (e.g., rule is 0.0, actual is 0)
    rule_float = coerce_to_float(rule_val)
    if rule_float is not None:
        return rule_float == ac_float

    return False

def match_fee_rule(context, rule):
    """
    Matches a specific transaction context against a fee rule.
    """
    # 1. Card Scheme (Exact Match)
    if rule.get('card_scheme') and rule['card_scheme'] != context['card_scheme']:
        return False

    # 2. Account Type (List Match - Wildcard if empty)
    if rule.get('account_type'):
        if context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List Match - Wildcard if empty)
    if rule.get('merchant_category_code'):
        # context['mcc'] is int, rule list contains ints
        if context['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean Match - Wildcard if None)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != context['is_credit']:
            return False

    # 5. ACI (List Match - Wildcard if empty)
    if rule.get('aci'):
        if context['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean/Float Match - Wildcard if None)
    # fees.json uses 1.0 (True), 0.0 (False), or None (Any)
    if rule.get('intracountry') is not None:
        rule_intra_bool = bool(float(rule['intracountry']))
        if rule_intra_bool != context['intracountry']:
            return False

    # 7. Monthly Volume (Range Match)
    if rule.get('monthly_volume'):
        if not parse_range(rule['monthly_volume'], context['monthly_volume']):
            return False

    # 8. Monthly Fraud Level (Range Match)
    if rule.get('monthly_fraud_level'):
        if not parse_range(rule['monthly_fraud_level'], context['monthly_fraud_rate']):
            return False

    # 9. Capture Delay (Range/String Match)
    if rule.get('capture_delay'):
        if not parse_range(rule['capture_delay'], context['capture_delay']):
            return False

    return True

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Belles_cookbook_store in October 2023
target_merchant = 'Belles_cookbook_store'
target_year = 2023
oct_start = 274
oct_end = 304

# Filter DataFrame
df_filtered = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= oct_start) &
    (df_payments['day_of_year'] <= oct_end)
].copy()

if df_filtered.empty:
    print("No transactions found for the specified merchant and period.")
    exit()

# 3. Get Merchant Attributes
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

merchant_mcc = merchant_info['merchant_category_code']
merchant_account_type = merchant_info['account_type']
merchant_capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats (Volume and Fraud)
total_volume = df_filtered['eur_amount'].sum()
fraud_txs = df_filtered[df_filtered['has_fraudulent_dispute'] == True]
fraud_volume = fraud_txs['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

# 5. Identify Applicable Fees
applicable_fee_ids = set()

# Extract unique transaction profiles to optimize matching
# These are the dynamic fields that vary per transaction
unique_profiles = df_filtered[['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country']].drop_duplicates()

for _, row in unique_profiles.iterrows():
    # Derive intracountry status
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
    # Build Context
    context = {
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': is_intracountry,
        'mcc': merchant_mcc,
        'account_type': merchant_account_type,
        'capture_delay': merchant_capture_delay,
        'monthly_volume': total_volume,
        'monthly_fraud_rate': fraud_rate
    }
    
    # Check against all fee rules
    for rule in fees_data:
        if match_fee_rule(context, rule):
            applicable_fee_ids.add(rule['ID'])

# 6. Output Result
sorted_ids = sorted(list(applicable_fee_ids))
result_string = ", ".join(map(str, sorted_ids))

print(result_string)
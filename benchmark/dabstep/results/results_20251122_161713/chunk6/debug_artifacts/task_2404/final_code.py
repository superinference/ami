import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

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

def parse_range_check(value, rule_string):
    """
    Checks if a numeric value fits within a rule string like '>5', '<3', '3-5', '100k-1m'.
    Returns True/False.
    """
    if rule_string is None:
        return True
    
    try:
        val = float(value)
    except (ValueError, TypeError):
        return False

    s = str(rule_string).strip().lower()
    
    # Handle k/m suffixes
    def parse_num(n_str):
        n_str = n_str.replace('k', '000').replace('m', '000000').replace('%', '')
        return float(n_str)

    if s.startswith('>'):
        limit = parse_num(s[1:])
        return val > limit
    elif s.startswith('<'):
        limit = parse_num(s[1:])
        return val < limit
    elif '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            return low <= val <= high
    else:
        # Exact match or immediate/manual which are strings, handled elsewhere usually
        # But if it's a number string:
        try:
            return val == parse_num(s)
        except:
            return False
    return False

def match_fee_rule(tx_dict, rule, merchant_attrs):
    """
    Determines if a transaction matches a specific fee rule.
    tx_dict: dictionary of transaction row
    rule: dictionary of fee rule
    merchant_attrs: dictionary of merchant specific attributes (mcc, account_type, capture_delay)
    """
    
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_dict.get('card_scheme'):
        return False

    # 2. Account Type (Merchant Attribute)
    # Rule list empty = matches all. Otherwise merchant's type must be in list.
    if is_not_empty(rule.get('account_type')):
        if merchant_attrs.get('account_type') not in rule['account_type']:
            return False

    # 3. Capture Delay (Merchant Attribute)
    # Rule null = matches all.
    if rule.get('capture_delay'):
        # capture_delay in merchant_data is a string (e.g., "manual", "immediate", "1")
        # capture_delay in fees is a rule string (e.g., "manual", ">5", "3-5")
        m_delay = str(merchant_attrs.get('capture_delay'))
        r_delay = str(rule['capture_delay'])
        
        # Direct string match
        if m_delay == r_delay:
            pass
        # Numeric range check if merchant delay is numeric
        elif m_delay.replace('.','',1).isdigit() and any(c in r_delay for c in ['<', '>', '-']):
            if not parse_range_check(m_delay, r_delay):
                return False
        # If rule is specific string (e.g. 'manual') and merchant is different
        elif m_delay != r_delay and not any(c in r_delay for c in ['<', '>', '-']):
            return False

    # 4. Merchant Category Code (Merchant Attribute)
    if is_not_empty(rule.get('merchant_category_code')):
        if merchant_attrs.get('merchant_category_code') not in rule['merchant_category_code']:
            return False

    # 5. Is Credit (Transaction Attribute)
    # Rule null = matches all.
    if rule.get('is_credit') is not None:
        # Ensure boolean comparison
        tx_credit = bool(tx_dict.get('is_credit'))
        rule_credit = bool(rule['is_credit'])
        if tx_credit != rule_credit:
            return False

    # 6. ACI (Transaction Attribute)
    if is_not_empty(rule.get('aci')):
        if tx_dict.get('aci') not in rule['aci']:
            return False

    # 7. Intracountry (Transaction Attribute)
    # Rule null = matches all.
    if rule.get('intracountry') is not None:
        # Intracountry definition: issuing_country == acquirer_country
        is_intra = (tx_dict.get('issuing_country') == tx_dict.get('acquirer_country'))
        
        # Rule is boolean (True/False) or 1.0/0.0
        rule_intra = bool(rule['intracountry'])
        
        if is_intra != rule_intra:
            return False

    # 8. Monthly Volume / Fraud (Merchant Context)
    # These are usually tier definitions. For this specific problem ("if fee 141 changed"),
    # we assume we are looking for transactions that fit the structural criteria of Fee 141.
    # However, strictly, we should check if the merchant qualifies.
    # Given the prompt context, we will check these if they are present in the rule.
    
    if rule.get('monthly_volume'):
        # We need the merchant's total monthly volume. 
        # Passed in merchant_attrs for efficiency? Or calculated?
        # For this specific task, we'll assume the merchant qualifies for the tier 
        # if the static attributes match, unless we have data to prove otherwise.
        # But let's check if provided in attrs.
        if 'monthly_volume_val' in merchant_attrs:
            if not parse_range_check(merchant_attrs['monthly_volume_val'], rule['monthly_volume']):
                return False

    if rule.get('monthly_fraud_level'):
        if 'monthly_fraud_rate' in merchant_attrs:
             if not parse_range_check(merchant_attrs['monthly_fraud_rate'], rule['monthly_fraud_level']):
                return False

    return True

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Get Merchant Attributes for "Rafa_AI"
target_merchant = "Rafa_AI"
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)

if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# Calculate Monthly Stats for Rafa_AI in June 2023 (Days 152-181)
# Note: Fee rules usually apply to the *previous* month's stats or current running, 
# but standard interpretation is the stats for the period in question.
# Let's calculate June stats to be precise for volume/fraud checks.
rafa_june_txs = df[
    (df['merchant'] == target_merchant) & 
    (df['day_of_year'] >= 152) & 
    (df['day_of_year'] <= 181)
]

total_volume_june = rafa_june_txs['eur_amount'].sum()
fraud_count = rafa_june_txs['has_fraudulent_dispute'].sum()
fraud_rate_june = (fraud_count / len(rafa_june_txs)) * 100 if len(rafa_june_txs) > 0 else 0.0
fraud_vol_june = rafa_june_txs[rafa_june_txs['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate_vol_june = (fraud_vol_june / total_volume_june) * 100 if total_volume_june > 0 else 0.0

# Add stats to merchant attributes for rule matching
merchant_attrs = {
    'merchant_category_code': merchant_info['merchant_category_code'],
    'account_type': merchant_info['account_type'],
    'capture_delay': merchant_info['capture_delay'],
    'monthly_volume_val': total_volume_june,
    'monthly_fraud_rate': fraud_rate_vol_june # Fees usually use volume-based fraud rate (see manual: "ratio between monthly total volume and monthly volume notified as fraud")
}

# 3. Get Fee Rule ID=141
fee_141 = next((f for f in fees if f['ID'] == 141), None)
if not fee_141:
    raise ValueError("Fee ID 141 not found in fees.json")

original_rate = fee_141['rate']
new_rate = 99

print(f"Analyzing Fee ID: 141")
print(f"Original Rate: {original_rate}")
print(f"New Rate: {new_rate}")
print(f"Merchant Attributes: {merchant_attrs}")
print(f"Fee 141 Criteria: {fee_141}")

# 4. Filter Transactions and Calculate Delta
# We iterate through Rafa_AI's June transactions and check if Fee 141 applies.
matching_amount_sum = 0.0
matching_count = 0

for _, tx in rafa_june_txs.iterrows():
    tx_dict = tx.to_dict()
    
    if match_fee_rule(tx_dict, fee_141, merchant_attrs):
        matching_amount_sum += tx['eur_amount']
        matching_count += 1

print(f"Matching Transactions: {matching_count}")
print(f"Matching Volume: {matching_amount_sum:.2f}")

# 5. Calculate Delta
# Formula: Fee = Fixed + (Rate * Amount / 10000)
# Delta = (New_Rate - Old_Rate) * Amount / 10000
# Fixed amount cancels out.

rate_diff = new_rate - original_rate
delta = (rate_diff * matching_amount_sum) / 10000

print(f"Delta Calculation: ({new_rate} - {original_rate}) * {matching_amount_sum:.2f} / 10000")
print(f"{delta:.14f}")
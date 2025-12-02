import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """
    Convert string with %, $, commas, k, m to float.
    Handles '8.3%' -> 0.083, '100k' -> 100000.
    """
    if pd.isna(value) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        # Clean string
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        # Remove comparison operators for pure number conversion
        v_clean = v.lstrip('><≤≥')
        
        try:
            if '%' in v_clean:
                return float(v_clean.replace('%', '')) / 100
            if 'k' in v_clean:
                return float(v_clean.replace('k', '')) * 1000
            if 'm' in v_clean:
                return float(v_clean.replace('m', '')) * 1000000
            return float(v_clean)
        except ValueError:
            return 0.0
    return 0.0

def parse_range(rule_value, actual_value):
    """
    Check if actual_value fits into rule_value range.
    rule_value examples: '100k-1m', '>8.3%', '3-5', 'immediate', 'manual'
    actual_value: float (for metrics) or string (for categorical)
    """
    if rule_value is None:
        return True
    
    # 1. Handle Categorical Exact Matches (e.g., 'immediate', 'manual')
    # If both are strings and no digits are involved, do direct comparison
    if isinstance(rule_value, str) and isinstance(actual_value, str):
        # Check if it looks like a non-numeric rule
        if not any(c.isdigit() for c in rule_value):
            return rule_value.lower() == actual_value.lower()

    # 2. Convert actual_value to float for numeric comparison
    try:
        actual_num = float(actual_value)
    except (ValueError, TypeError):
        # If actual value isn't a number (e.g. 'manual') but rule expects number/range, return False
        return False

    # 3. Handle Numeric Rules
    if isinstance(rule_value, str):
        # Inequalities
        if rule_value.startswith('>'):
            limit = coerce_to_float(rule_value[1:])
            return actual_num > limit
        if rule_value.startswith('<'):
            limit = coerce_to_float(rule_value[1:])
            return actual_num < limit
        
        # Ranges "min-max"
        if '-' in rule_value:
            parts = rule_value.split('-')
            if len(parts) == 2:
                min_val = coerce_to_float(parts[0])
                max_val = coerce_to_float(parts[1])
                return min_val <= actual_num <= max_val

    # 4. Fallback: Exact Numeric Match
    return actual_num == coerce_to_float(rule_value)

def match_fee_rule(transaction_context, rule):
    """
    Check if a fee rule applies to a specific transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != transaction_context.get('card_scheme'):
        return False

    # 2. Account Type (List match, empty = wildcard)
    if rule.get('account_type'):
        if transaction_context.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if transaction_context.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match, None = wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != transaction_context.get('is_credit'):
            return False

    # 5. ACI (List match, empty = wildcard)
    if rule.get('aci'):
        if transaction_context.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match, None = wildcard)
    if rule.get('intracountry') is not None:
        # Rule uses 0.0/1.0, Context uses True/False
        rule_intra = bool(rule['intracountry'])
        ctx_intra = bool(transaction_context.get('intracountry'))
        if rule_intra != ctx_intra:
            return False

    # 7. Capture Delay (Range/Exact match)
    if rule.get('capture_delay'):
        if not parse_range(rule['capture_delay'], transaction_context.get('capture_delay')):
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range(rule['monthly_volume'], transaction_context.get('monthly_volume')):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        # Context fraud level is a float ratio (e.g., 0.0841)
        # Rule might be ">8.3%"
        if not parse_range(rule['monthly_fraud_level'], transaction_context.get('monthly_fraud_level')):
            return False

    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# Paths
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

# Load Data
df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)

# Target Parameters
target_merchant = 'Golfclub_Baron_Friso'
target_day = 365
target_year = 2023

# 1. Get Merchant Static Data
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found.")
    exit()

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 2. Calculate Monthly Metrics (December 2023)
# December is days 335 to 365 inclusive (non-leap year)
december_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= 335) &
    (df_payments['day_of_year'] <= 365)
]

monthly_volume = december_txs['eur_amount'].sum()
fraud_volume = december_txs[december_txs['has_fraudulent_dispute']]['eur_amount'].sum()
# Fraud level is ratio of fraud volume to total volume
monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 3. Filter Transactions for the Specific Day (365)
day_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
].copy()

if day_txs.empty:
    print("No transactions found for this merchant on day 365.")
    exit()

# Calculate intracountry for these transactions
# True if issuing_country == acquirer_country
day_txs['intracountry'] = day_txs['issuing_country'] == day_txs['acquirer_country']

# 4. Find Applicable Fee IDs
applicable_fee_ids = set()

# We iterate through unique profiles to save time
# Fee determinants from transaction: card_scheme, is_credit, aci, intracountry
unique_profiles = day_txs[['card_scheme', 'is_credit', 'aci', 'intracountry']].drop_duplicates()

for _, row in unique_profiles.iterrows():
    # Construct transaction context dictionary
    tx_context = {
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['intracountry'],
        'mcc': mcc,
        'account_type': account_type,
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    # Check against all fee rules
    for rule in fees:
        if match_fee_rule(tx_context, rule):
            applicable_fee_ids.add(rule['ID'])

# 5. Output Result
sorted_ids = sorted(list(applicable_fee_ids))
print(", ".join(map(str, sorted_ids)))
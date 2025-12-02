# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1705
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8041 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import re

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        # Handle percentages
        if '%' in v:
            return float(v.replace('%', '')) / 100.0
        # Handle k/m suffixes
        if v.lower().endswith('k'):
            return float(v[:-1]) * 1000
        if v.lower().endswith('m'):
            return float(v[:-1]) * 1000000
        # Handle comparison operators for direct conversion (stripping them for raw value if needed, 
        # though range matching logic usually handles the operators separately)
        v_clean = v.lstrip('><≤≥=')
        try:
            return float(v_clean)
        except ValueError:
            return 0.0
    return 0.0

def parse_range_check(value, rule_string):
    """
    Checks if a numeric value fits within a rule string (e.g., '100k-1m', '>5', '<3').
    Returns True if match, False otherwise.
    """
    if rule_string is None:
        return True
    
    # Handle range "min-max"
    if '-' in rule_string:
        try:
            parts = rule_string.split('-')
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return min_val <= value <= max_val
        except:
            return False
            
    # Handle inequalities
    s = rule_string.strip()
    if s.startswith('>='):
        return value >= coerce_to_float(s[2:])
    elif s.startswith('>'):
        return value > coerce_to_float(s[1:])
    elif s.startswith('<='):
        return value <= coerce_to_float(s[2:])
    elif s.startswith('<'):
        return value < coerce_to_float(s[1:])
    
    # Exact match (numeric string)
    try:
        return value == coerce_to_float(s)
    except:
        return False

def check_capture_delay(merchant_delay, rule_delay):
    """
    Matches merchant capture delay (e.g., '1', 'manual') against rule (e.g., '<3', 'manual').
    """
    if rule_delay is None:
        return True
    
    # Exact string match (e.g., 'manual' == 'manual')
    if str(merchant_delay).lower() == str(rule_delay).lower():
        return True
        
    # If merchant delay is numeric (e.g., '1'), check against numeric rule (e.g., '<3')
    try:
        val = float(merchant_delay)
        return parse_range_check(val, rule_delay)
    except ValueError:
        # Merchant delay is non-numeric (e.g. 'immediate', 'manual') and didn't match exact string above
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_context must contain:
      - card_scheme (str)
      - is_credit (bool)
      - aci (str)
      - intracountry (bool)
      - account_type (str)
      - merchant_category_code (int)
      - capture_delay (str)
      - monthly_volume (float)
      - monthly_fraud_level (float)
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match or Wildcard)
    if rule['account_type'] and tx_context['account_type'] not in rule['account_type']:
        return False

    # 3. Merchant Category Code (List match or Wildcard)
    if rule['merchant_category_code'] and tx_context['merchant_category_code'] not in rule['merchant_category_code']:
        return False

    # 4. Is Credit (Bool match or Wildcard)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_context['is_credit']:
        return False

    # 5. ACI (List match or Wildcard)
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False

    # 6. Intracountry (Bool match or Wildcard)
    # Note: JSON uses 0.0/1.0 for boolean often, need to handle types carefully
    if rule['intracountry'] is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    # 7. Monthly Volume (Range match or Wildcard)
    if rule['monthly_volume'] is not None:
        if not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 8. Monthly Fraud Level (Range match or Wildcard)
    if rule['monthly_fraud_level'] is not None:
        # Fraud level in context is 0.116 (11.6%), rule might be "10%-20%"
        if not parse_range_check(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    # 9. Capture Delay (Complex match or Wildcard)
    if rule['capture_delay'] is not None:
        if not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
            return False

    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data_list = json.load(f)

# 2. Define Target
target_merchant = 'Rafa_AI'
target_year = 2023
target_day = 10

# 3. Get Merchant Metadata
merchant_meta = next((m for m in merchant_data_list if m['merchant'] == target_merchant), None)
if not merchant_meta:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

# 4. Calculate Monthly Stats (January 2023)
# Day 10 is in January. Filter for Jan 1 - Jan 31.
df_merchant_2023 = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
]
df_jan = df_merchant_2023[(df_merchant_2023['day_of_year'] >= 1) & (df_merchant_2023['day_of_year'] <= 31)]

monthly_volume = df_jan['eur_amount'].sum()
fraud_volume = df_jan[df_jan['has_fraudulent_dispute'] == True]['eur_amount'].sum()
monthly_fraud_rate = (fraud_volume / monthly_volume) if monthly_volume > 0 else 0.0

# 5. Get Transactions for the Specific Day
df_day = df_merchant_2023[df_merchant_2023['day_of_year'] == target_day].copy()

# 6. Identify Applicable Fee IDs
applicable_fee_ids = set()

# We iterate through each transaction on that day to find which fees apply.
# Optimization: Group by unique attributes first to avoid checking same rules 1000s of times.
# Attributes that vary per transaction: card_scheme, is_credit, aci, intracountry (derived)
df_day['intracountry'] = df_day['issuing_country'] == df_day['acquirer_country']

unique_tx_profiles = df_day[[
    'card_scheme', 'is_credit', 'aci', 'intracountry'
]].drop_duplicates()

# Context that is constant for the merchant/month
base_context = {
    'account_type': merchant_meta['account_type'],
    'merchant_category_code': merchant_meta['merchant_category_code'],
    'capture_delay': merchant_meta['capture_delay'],
    'monthly_volume': monthly_volume,
    'monthly_fraud_level': monthly_fraud_rate
}

# Iterate profiles
for _, row in unique_tx_profiles.iterrows():
    # Build full context for this transaction type
    tx_context = base_context.copy()
    tx_context.update({
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['intracountry']
    })
    
    # Check against all fee rules
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            applicable_fee_ids.add(rule['ID'])

# 7. Output Result
# The question asks "what are the Fee IDs". We return a sorted list.
sorted_ids = sorted(list(applicable_fee_ids))
print(", ".join(map(str, sorted_ids)))

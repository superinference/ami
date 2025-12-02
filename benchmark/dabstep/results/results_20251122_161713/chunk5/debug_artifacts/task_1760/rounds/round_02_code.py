# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1760
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7746 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import re

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

def parse_range(rule_value, actual_value):
    """
    Checks if actual_value fits into the rule_value range/condition.
    rule_value examples: "100k-1m", ">5", "<3", "immediate", "manual", "7.7%-8.3%"
    actual_value: float or string
    """
    if rule_value is None:
        return True
    
    # Handle exact string matches (e.g., "manual", "immediate")
    if isinstance(rule_value, str) and isinstance(actual_value, str):
        if rule_value.lower() == actual_value.lower():
            return True
            
    # Convert actual_value to float if possible for numeric comparisons
    try:
        actual_float = float(actual_value)
    except (ValueError, TypeError):
        actual_float = None

    # Handle "k" and "m" suffixes in rule
    rule_str = str(rule_value).lower().replace(',', '')
    
    # Range check (e.g., "100k-1m", "0.0%-0.8%")
    if '-' in rule_str:
        try:
            parts = rule_str.split('-')
            min_s = parts[0].strip()
            max_s = parts[1].strip()
            
            # Helper to parse "100k", "8.3%"
            def parse_num(s):
                if '%' in s: return float(s.replace('%', '')) / 100
                mult = 1
                if 'k' in s: mult = 1000; s = s.replace('k', '')
                if 'm' in s: mult = 1000000; s = s.replace('m', '')
                return float(s) * mult

            min_val = parse_num(min_s)
            max_val = parse_num(max_s)
            
            if actual_float is not None:
                return min_val <= actual_float <= max_val
        except:
            pass

    # Inequality check (e.g., ">5", "<3", ">8.3%")
    if actual_float is not None:
        if rule_str.startswith('>'):
            val = rule_str[1:]
            # Handle percentage in inequality
            if '%' in val:
                limit = float(val.replace('%', '')) / 100
            else:
                limit = float(val)
            return actual_float > limit
        if rule_str.startswith('<'):
            val = rule_str[1:]
            if '%' in val:
                limit = float(val.replace('%', '')) / 100
            else:
                limit = float(val)
            return actual_float < limit

    return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_context: dict containing transaction and merchant details
    rule: dict representing a fee rule
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match - Wildcard if empty/None)
    if rule['account_type']:
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match - Wildcard if empty/None)
    if rule['merchant_category_code']:
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match - Wildcard if None)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 5. ACI (List match - Wildcard if empty/None)
    if rule['aci']:
        if tx_context['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match - Wildcard if None)
    # Note: fees.json uses 0.0/1.0 for boolean false/true sometimes, or boolean types
    if rule['intracountry'] is not None:
        rule_intra = bool(rule['intracountry']) # Convert 0.0/1.0 to False/True
        if rule_intra != tx_context['intracountry']:
            return False

    # 7. Monthly Volume (Range match)
    if rule['monthly_volume']:
        if not parse_range(rule['monthly_volume'], tx_context['monthly_volume']):
            return False

    # 8. Monthly Fraud Level (Range match)
    if rule['monthly_fraud_level']:
        if not parse_range(rule['monthly_fraud_level'], tx_context['monthly_fraud_rate']):
            return False

    # 9. Capture Delay (Range/Value match)
    if rule['capture_delay']:
        if not parse_range(rule['capture_delay'], tx_context['capture_delay']):
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

df_filtered = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= oct_start) &
    (df_payments['day_of_year'] <= oct_end)
].copy()

# 3. Get Merchant Attributes
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

merchant_mcc = merchant_info['merchant_category_code']
merchant_account_type = merchant_info['account_type']
merchant_capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats (Volume and Fraud)
# Note: Fraud rate is typically Fraud Volume / Total Volume
total_volume = df_filtered['eur_amount'].sum()
fraud_volume = df_filtered[df_filtered['has_fraudulent_dispute'] == True]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

# 5. Identify Applicable Fees
applicable_fee_ids = set()

# Optimization: Iterate over unique transaction profiles instead of every row
# A profile consists of the fields that vary per transaction and affect fee rules
# Fields: card_scheme, is_credit, aci, issuing_country, acquirer_country
unique_profiles = df_filtered[['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country']].drop_duplicates()

for _, row in unique_profiles.iterrows():
    # Construct context for this transaction profile
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
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

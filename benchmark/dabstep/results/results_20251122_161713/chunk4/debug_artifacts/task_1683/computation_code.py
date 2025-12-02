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
    return 0.0

def parse_value_with_suffix(val_str):
    """Parses strings like '100k', '1m', '8.3%' into floats."""
    s = str(val_str).lower().strip()
    scale = 1.0
    
    if 'k' in s:
        scale = 1000.0
        s = s.replace('k', '')
    elif 'm' in s:
        scale = 1000000.0
        s = s.replace('m', '')
    elif '%' in s:
        scale = 0.01
        s = s.replace('%', '')
        
    try:
        return float(s) * scale
    except:
        return None

def parse_range_check(rule_val, actual_val):
    """
    Checks if actual_val falls within rule_val range.
    rule_val examples: "100k-1m", ">5", "<3", "7.7%-8.3%", "manual"
    actual_val: float or string
    """
    if rule_val is None:
        return True
    
    # Exact string match (e.g., "manual", "immediate")
    if isinstance(rule_val, str) and isinstance(actual_val, str):
        if rule_val.lower() == actual_val.lower():
            return True
            
    # Convert actual_val to float for numeric comparisons
    try:
        actual_float = float(actual_val)
    except (ValueError, TypeError):
        # If actual is not numeric (e.g. "manual") and didn't match string above, it's a mismatch
        return False

    s = str(rule_val).lower().strip()
    
    # Range "min-max"
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            low = parse_value_with_suffix(parts[0])
            high = parse_value_with_suffix(parts[1])
            if low is not None and high is not None:
                return low <= actual_float <= high
                
    # Inequality ">val"
    if s.startswith('>'):
        val = parse_value_with_suffix(s[1:])
        if val is not None:
            return actual_float > val
            
    # Inequality "<val"
    if s.startswith('<'):
        val = parse_value_with_suffix(s[1:])
        if val is not None:
            return actual_float < val
            
    # Single numeric value match (rare for ranges)
    val = parse_value_with_suffix(s)
    if val is not None:
        return actual_float == val
        
    return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List containment)
    # Wildcard: [] or None matches all
    if rule.get('account_type') and tx_context['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List containment)
    if rule.get('merchant_category_code') and tx_context['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 4. Is Credit (Boolean match)
    # Wildcard: None matches all
    if rule.get('is_credit') is not None and rule['is_credit'] != tx_context['is_credit']:
        return False
        
    # 5. ACI (List containment)
    if rule.get('aci') and tx_context['aci'] not in rule['aci']:
        return False
        
    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None and rule['intracountry'] != tx_context['intracountry']:
        return False
        
    # 7. Capture Delay (Range/String check)
    if not parse_range_check(rule.get('capture_delay'), tx_context['capture_delay']):
        return False
        
    # 8. Monthly Volume (Range check)
    if not parse_range_check(rule.get('monthly_volume'), tx_context['monthly_volume']):
        return False
        
    # 9. Monthly Fraud Level (Range check)
    if not parse_range_check(rule.get('monthly_fraud_level'), tx_context['monthly_fraud_level']):
        return False
        
    return True

# --- Main Execution ---

# File paths
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'

# Load data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# Target parameters
target_merchant = 'Belles_cookbook_store'
target_year = 2023
target_day = 100

# 1. Get Merchant Attributes
# Find the merchant in merchant_data.json
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant '{target_merchant}' not found in merchant_data.json")
    exit()

m_account_type = merchant_info.get('account_type')
m_mcc = merchant_info.get('merchant_category_code')
m_capture_delay = merchant_info.get('capture_delay')

# 2. Calculate Monthly Stats for April 2023
# Day 100 falls in April (Jan=31, Feb=28, Mar=31 -> 90 days. April is 91-120).
# Manual: "Monthly volumes and rates are computed always in natural months"
april_mask = (
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= 91) &
    (df_payments['day_of_year'] <= 120)
)
df_april = df_payments[april_mask]

# Calculate Volume and Fraud Rate
monthly_volume = df_april['eur_amount'].sum()
fraud_volume = df_april[df_april['has_fraudulent_dispute'] == True]['eur_amount'].sum()

# Fraud rate is ratio of fraud volume to total volume
monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 3. Filter Transactions for the Specific Day (Day 100)
day_mask = (
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
)
target_transactions = df_payments[day_mask]

# 4. Find Applicable Fee IDs
applicable_ids = set()

for _, tx in target_transactions.iterrows():
    # Determine intracountry status
    # True if issuing_country == acquirer_country
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    # Build context for this transaction
    context = {
        'card_scheme': tx['card_scheme'],
        'account_type': m_account_type,
        'mcc': m_mcc,
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': is_intracountry,
        'capture_delay': m_capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    # Check against all fee rules
    for rule in fees_data:
        if match_fee_rule(context, rule):
            applicable_ids.add(rule['ID'])

# 5. Output Results
# Sort IDs for consistent output
sorted_ids = sorted(list(applicable_ids))
print(", ".join(map(str, sorted_ids)))
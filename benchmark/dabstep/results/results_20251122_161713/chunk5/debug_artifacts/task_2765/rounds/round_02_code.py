# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2765
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7186 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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

def parse_range(rule_val, actual_val):
    """
    Parses rule strings like '100k-1m', '>5', '<3', '7.7%-8.3%'.
    Returns True if actual_val fits, False otherwise.
    Handles None/Null in rule as wildcard (True).
    """
    if rule_val is None:
        return True
    
    s = str(rule_val).strip().lower()
    
    # Handle percentages
    is_pct = '%' in s
    
    # Helper to parse number
    def parse_num(n_str):
        n_str = n_str.replace('%', '').replace('k', '000').replace('m', '000000')
        return float(n_str) if not is_pct else float(n_str) / 100

    try:
        if '-' in s:
            low, high = s.split('-')
            return parse_num(low) <= actual_val <= parse_num(high)
        elif s.startswith('>'):
            return actual_val > parse_num(s[1:])
        elif s.startswith('<'):
            return actual_val < parse_num(s[1:])
        else:
            # Exact match for numbers (with tolerance)
            return abs(actual_val - parse_num(s)) < 1e-9
    except:
        return False

def check_static_match(rule, merchant_context):
    """Checks if a fee rule matches the merchant's static attributes."""
    # Merchant Context: mcc, account_type, monthly_volume, monthly_fraud_rate, capture_delay
    
    # MCC (List in rule)
    if rule['merchant_category_code'] is not None:
        if merchant_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # Account Type (List in rule)
    if rule['account_type'] is not None and len(rule['account_type']) > 0:
        if merchant_context['account_type'] not in rule['account_type']:
            return False
            
    # Capture Delay (String in rule)
    if rule['capture_delay'] is not None:
        # Exact match for string (case-insensitive)
        if str(rule['capture_delay']).lower() != str(merchant_context['capture_delay']).lower():
            return False
            
    # Monthly Volume (Range string in rule)
    if rule['monthly_volume'] is not None:
        if not parse_range(rule['monthly_volume'], merchant_context['monthly_volume']):
            return False
            
    # Monthly Fraud Level (Range string in rule)
    if rule['monthly_fraud_level'] is not None:
        if not parse_range(rule['monthly_fraud_level'], merchant_context['monthly_fraud_rate']):
            return False
            
    return True

def check_dynamic_match(rule, tx_context):
    """Checks if a fee rule matches the transaction's dynamic attributes."""
    # Tx Context: is_credit, aci, intracountry
    
    # Is Credit (Bool/None)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # ACI (List/None)
    if rule['aci'] is not None and len(rule['aci']) > 0:
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # Intracountry (Bool/None)
    # fees.json uses 0.0 (False) or 1.0 (True) or None
    if rule['intracountry'] is not None:
        rule_intra = (float(rule['intracountry']) == 1.0)
        if rule_intra != tx_context['intracountry']:
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculates the fee amount based on the rule."""
    fixed = float(rule['fixed_amount'])
    rate = float(rule['rate'])
    return fixed + (rate * amount / 10000)

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant 'Rafa_AI' and Year 2023
merchant_name = 'Rafa_AI'
df_rafa = df_payments[(df_payments['merchant'] == merchant_name) & (df_payments['year'] == 2023)].copy()

# 3. Calculate Merchant Static Stats (Volume, Fraud, etc.)
total_volume_year = df_rafa['eur_amount'].sum()
avg_monthly_volume = total_volume_year / 12

fraud_volume = df_rafa[df_rafa['has_fraudulent_dispute'] == True]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume_year if total_volume_year > 0 else 0.0

# Get Merchant Metadata
m_info = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
if not m_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

merchant_context = {
    'mcc': m_info['merchant_category_code'],
    'account_type': m_info['account_type'],
    'monthly_volume': avg_monthly_volume,
    'monthly_fraud_rate': fraud_rate,
    'capture_delay': m_info['capture_delay']
}

# 4. Pre-filter fees by Scheme and Static Attributes
# We want to test all 4 schemes to see which one is most expensive
schemes = ['GlobalCard', 'NexPay', 'SwiftCharge', 'TransactPlus']
filtered_fees = {s: [] for s in schemes}

for rule in fees:
    s = rule['card_scheme']
    if s in schemes:
        if check_static_match(rule, merchant_context):
            filtered_fees[s].append(rule)

# 5. Calculate Total Fees for each Scheme
# We simulate processing ALL of Rafa_AI's 2023 transactions through each scheme
# Pre-calculate dynamic columns for speed
df_rafa['intracountry'] = df_rafa['issuing_country'] == df_rafa['acquirer_country']

results = {}
# Convert to records for faster iteration than iterrows
tx_records = df_rafa[['eur_amount', 'is_credit', 'aci', 'intracountry']].to_dict('records')

for scheme in schemes:
    # Sort rules by ID to ensure deterministic "first match" priority
    scheme_rules = sorted(filtered_fees[scheme], key=lambda x: x['ID'])
    
    if not scheme_rules:
        results[scheme] = 0.0
        continue
        
    total_scheme_fee = 0.0
    
    for tx in tx_records:
        # Find first matching rule for this transaction
        matched_rule = None
        for rule in scheme_rules:
            if check_dynamic_match(rule, tx):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_scheme_fee += fee
            
    results[scheme] = total_scheme_fee

# 6. Identify the Maximum
max_scheme = max(results, key=results.get)
max_fee = results[max_scheme]

# Output the result
print(max_scheme)

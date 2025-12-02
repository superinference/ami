# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1712
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: insufficient
# Plan steps: 2
# Code length: 8293 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        clean_v = v.lstrip('><≤≥')
        try:
            return float(clean_v)
        except ValueError:
            pass
    return None

def parse_volume_range(range_str):
    """Parses volume strings like '100k-1m', '>10m' into (min, max)."""
    if not range_str:
        return (0, float('inf'))
    
    s = str(range_str).lower().replace(',', '')
    
    def get_val(val_str):
        m = 1
        if 'k' in val_str:
            m = 1000
            val_str = val_str.replace('k', '')
        elif 'm' in val_str:
            m = 1000000
            val_str = val_str.replace('m', '')
        try:
            return float(val_str) * m
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return (get_val(parts[0]), get_val(parts[1]))
    elif '>' in s:
        return (get_val(s.replace('>', '')), float('inf'))
    elif '<' in s:
        return (0, get_val(s.replace('<', '')))
    return (0, float('inf'))

def parse_fraud_range(range_str):
    """Parses fraud strings like '7.7%-8.3%', '<5%' into (min, max)."""
    if not range_str:
        return (0, float('inf'))
    
    s = str(range_str).replace('%', '')
    
    if '-' in s:
        parts = s.split('-')
        return (float(parts[0])/100, float(parts[1])/100)
    elif '>' in s:
        return (float(s.replace('>', ''))/100, float('inf'))
    elif '<' in s:
        return (0, float(s.replace('<', ''))/100)
    return (0, float('inf'))

def parse_capture_delay(rule_val, merchant_val):
    """Checks if merchant capture delay matches rule."""
    if rule_val is None:
        return True
    
    # Direct string match
    if str(rule_val) == str(merchant_val):
        return True
        
    # Numeric comparison if possible
    try:
        # Handle 'manual', 'immediate' which are strings
        if str(merchant_val).lower() in ['manual', 'immediate']:
            return str(rule_val).lower() == str(merchant_val).lower()

        m_val = float(merchant_val)
        if '-' in str(rule_val):
            low, high = map(float, str(rule_val).split('-'))
            return low <= m_val <= high
        elif '>' in str(rule_val):
            limit = float(str(rule_val).replace('>', ''))
            return m_val > limit
        elif '<' in str(rule_val):
            limit = float(str(rule_val).replace('<', ''))
            return m_val < limit
    except (ValueError, TypeError):
        pass
        
    return False

def match_fee_rule(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    """
    
    # 1. Card Scheme (Exact match required)
    if rule.get('card_scheme') != tx_context['card_scheme']:
        return False

    # 2. Merchant Category Code (List containment)
    # Rule has list of ints, context has int
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 3. Account Type (List containment)
    # Rule has list of strings, context has string
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 4. Capture Delay (Complex match)
    if not parse_capture_delay(rule.get('capture_delay'), tx_context['capture_delay']):
        return False

    # 5. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_vol, max_vol = parse_volume_range(rule['monthly_volume'])
        if not (min_vol <= tx_context['monthly_volume'] <= max_vol):
            return False

    # 6. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_fraud, max_fraud = parse_fraud_range(rule['monthly_fraud_level'])
        # Use small epsilon for float comparison
        if not (min_fraud - 1e-9 <= tx_context['monthly_fraud_rate'] <= max_fraud + 1e-9):
            return False

    # 7. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 8. ACI (List containment)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Boolean match)
    # Rule: 1.0/0.0/None. Context: True/False
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    # fee = fixed_amount + rate * transaction_value / 10000
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000)

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

# File paths
payments_path = '/output/chunk1/data/context/payments.csv'
fees_path = '/output/chunk1/data/context/fees.json'
merchant_path = '/output/chunk1/data/context/merchant_data.json'

# Load data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# Target Merchant and Date
target_merchant = 'Belles_cookbook_store'
target_year = 2023
target_day = 12

# 1. Get Merchant Profile
merchant_profile = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_profile:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 2. Calculate Monthly Stats (January 2023)
# Day 12 is in January (Days 1-31)
jan_start = 1
jan_end = 31

jan_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= jan_start) &
    (df_payments['day_of_year'] <= jan_end)
]

monthly_volume = jan_txs['eur_amount'].sum()

# Fraud Rate Calculation:
# Manual Section 7: "Fraud is defined as the ratio of fraudulent volume over total volume."
fraud_volume = jan_txs[jan_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

print(f"Merchant: {target_merchant}")
print(f"Monthly Volume (Jan): {monthly_volume}")
print(f"Monthly Fraud Rate: {monthly_fraud_rate}")

# 3. Filter Target Transactions (Day 12)
day_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day)
]

print(f"Transactions on Day {target_day}: {len(day_txs)}")

# 4. Calculate Fees
total_fees = 0.0
matched_count = 0

for idx, tx in day_txs.iterrows():
    # Build Context
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    context = {
        'card_scheme': tx['card_scheme'],
        'mcc': merchant_profile['merchant_category_code'],
        'account_type': merchant_profile['account_type'],
        'capture_delay': merchant_profile['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate,
        'is_credit': bool(tx['is_credit']),
        'aci': tx['aci'],
        'intracountry': is_intracountry
    }
    
    # Find Match
    matched_rule = None
    # Iterate through fees to find the first match
    for rule in fees_data:
        if match_fee_rule(context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee
        matched_count += 1
    else:
        # Fallback or error logging if needed
        pass

print(f"Total Fees: {total_fees}")

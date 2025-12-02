# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1868
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 8090 characters (FULL CODE)
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
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range(range_str):
    """Parses a range string like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle suffixes
    multiplier = 1
    if 'k' in s:
        multiplier = 1000
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000
        s = s.replace('m', '')
        
    if '%' in s:
        multiplier = 0.01
        s = s.replace('%', '')

    try:
        if '-' in s:
            parts = s.split('-')
            low = float(parts[0]) * multiplier
            high = float(parts[1]) * multiplier
            return low, high
        elif s.startswith('>'):
            val = float(s.replace('>', '')) * multiplier
            return val, float('inf')
        elif s.startswith('<'):
            val = float(s.replace('<', '')) * multiplier
            return float('-inf'), val
        else:
            # Exact match treated as range [val, val]
            val = float(s) * multiplier
            return val, val
    except:
        return None, None

def check_range(value, range_str):
    """Checks if a value falls within a string range."""
    if range_str is None:
        return True
    low, high = parse_range(range_str)
    if low is None: # Parsing failed or empty
        return True
    # Use a small epsilon for float comparison if needed, but direct comparison is usually sufficient here
    return low <= value <= high

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List match - empty list means wildcard)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match - empty list means wildcard)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match - null means wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 5. ACI (List match - empty list means wildcard)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match - null means wildcard)
    # Rule uses 0.0/1.0 or boolean. Tx uses boolean.
    if rule.get('intracountry') is not None:
        rule_intra = rule['intracountry']
        # Convert rule value to boolean (0.0 -> False, 1.0 -> True)
        if isinstance(rule_intra, (float, int)):
            rule_intra = bool(rule_intra)
        
        if rule_intra != tx_ctx['intracountry']:
            return False

    # 7. Capture Delay (String match or Range)
    if rule.get('capture_delay'):
        r_delay = str(rule['capture_delay'])
        m_delay = str(tx_ctx['capture_delay'])
        
        # Direct match
        if r_delay == m_delay:
            pass
        # Range match (e.g. >5, <3)
        elif r_delay.startswith('>') or r_delay.startswith('<') or '-' in r_delay:
            # Try to parse merchant delay as int
            if m_delay.isdigit():
                m_val = int(m_delay)
                low, high = parse_range(r_delay)
                if low is not None:
                    if not (low <= m_val <= high):
                        return False
                else:
                    return False # Failed to parse range
            else:
                # Merchant delay is 'manual' or 'immediate', rule is numeric range -> No match
                return False
        else:
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Rafa_AI and October 2023
merchant_name = 'Rafa_AI'
start_day = 274
end_day = 304

# Filter by merchant
df_rafa = df[df['merchant'] == merchant_name].copy()

# Filter by date (October)
df_oct = df_rafa[(df_rafa['day_of_year'] >= start_day) & (df_rafa['day_of_year'] <= end_day)].copy()

# 3. Get Merchant Context
rafa_info = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
if not rafa_info:
    # Fallback if exact name match fails (though it shouldn't based on exploration)
    print(f"Warning: {merchant_name} not found in merchant_data.json")
    # Assuming standard values or exiting
    exit()

account_type = rafa_info.get('account_type')
mcc = rafa_info.get('merchant_category_code')
capture_delay = rafa_info.get('capture_delay')

# 4. Calculate Monthly Stats (Volume and Fraud Rate)
# Volume
monthly_volume = df_oct['eur_amount'].sum()

# Fraud Volume (using 'has_fraudulent_dispute')
fraud_volume = df_oct[df_oct['has_fraudulent_dispute'] == True]['eur_amount'].sum()

# Fraud Rate (Ratio)
if monthly_volume > 0:
    monthly_fraud_rate = fraud_volume / monthly_volume
else:
    monthly_fraud_rate = 0.0

# 5. Calculate Fees per Transaction
total_fees = 0.0
matched_count = 0
unmatched_count = 0

# Pre-calculate intracountry
# Intracountry is True if issuing_country == acquirer_country
df_oct['intracountry'] = df_oct['issuing_country'] == df_oct['acquirer_country']

for _, row in df_oct.iterrows():
    # Build context for this transaction
    tx_ctx = {
        'card_scheme': row['card_scheme'],
        'account_type': account_type,
        'mcc': mcc,
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['intracountry'],
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Find matching rule
    matched_rule = None
    for rule in fees:
        if match_fee_rule(tx_ctx, rule):
            matched_rule = rule
            break
    
    if matched_rule:
        fee = calculate_fee(row['eur_amount'], matched_rule)
        total_fees += fee
        matched_count += 1
    else:
        unmatched_count += 1

# 6. Output Result
# The question asks for the total fees in euros.
print(f"{total_fees:.2f}")

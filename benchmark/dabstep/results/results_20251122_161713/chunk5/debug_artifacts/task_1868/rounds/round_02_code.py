# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1868
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8923 characters (FULL CODE)
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
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range(range_str, value_type='float'):
    """Parses a range string like '100k-1m' or '<5' into (min, max)."""
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
    # Handle edge cases for inclusive/exclusive if needed, but simple comparison usually suffices
    # For fraud rates, precision matters, so we use a small epsilon if needed, but standard comparison is fine here.
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

    # 2. Account Type (List match)
    # Rule has list of allowed types. Merchant has one type.
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Fee rule might specify 0.0 (False) or 1.0 (True) or boolean
        rule_intra = rule['intracountry']
        if isinstance(rule_intra, (float, int)):
            rule_intra = bool(rule_intra)
        if rule_intra != tx_ctx['intracountry']:
            return False

    # 7. Capture Delay (String match)
    if rule.get('capture_delay'):
        # Handle simple string match or range if applicable (though manual says string values)
        # Manual values: '3-5', '>5', '<3', 'immediate', 'manual'
        # Merchant values: 'manual', 'immediate', '1', etc.
        r_delay = str(rule['capture_delay'])
        m_delay = str(tx_ctx['capture_delay'])
        
        if r_delay == m_delay:
            pass # Match
        elif r_delay == '>5':
            if m_delay.isdigit() and int(m_delay) > 5: pass
            else: return False
        elif r_delay == '<3':
            if m_delay.isdigit() and int(m_delay) < 3: pass
            else: return False
        elif '-' in r_delay: # e.g. 3-5
            try:
                low, high = map(int, r_delay.split('-'))
                if m_delay.isdigit() and low <= int(m_delay) <= high: pass
                else: return False
            except:
                if r_delay != m_delay: return False
        else:
            if r_delay != m_delay: return False

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
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

account_type = rafa_info.get('account_type')
mcc = rafa_info.get('merchant_category_code')
capture_delay = rafa_info.get('capture_delay')

# 4. Calculate Monthly Stats (Volume and Fraud Rate)
# These are needed to select the correct fee tier.
monthly_volume = df_oct['eur_amount'].sum()
fraud_volume = df_oct[df_oct['has_fraudulent_dispute'] == True]['eur_amount'].sum()

# Fraud rate is Fraud Volume / Total Volume (ratio)
if monthly_volume > 0:
    monthly_fraud_rate = fraud_volume / monthly_volume
else:
    monthly_fraud_rate = 0.0

print(f"Merchant: {merchant_name}")
print(f"October Volume: €{monthly_volume:,.2f}")
print(f"October Fraud Volume: €{fraud_volume:,.2f}")
print(f"October Fraud Rate: {monthly_fraud_rate:.4%}")

# 5. Calculate Fees per Transaction
total_fees = 0.0
matched_count = 0
unmatched_count = 0

# Pre-calculate intracountry for efficiency
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
        'monthly_fraud_rate': monthly_fraud_rate # Passed as ratio (e.g., 0.083)
    }
    
    # Find matching rule
    # We iterate through fees and take the first one that matches ALL criteria.
    # (Assuming fees.json is ordered or rules are mutually exclusive enough for this purpose)
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
        # If no rule matches, we skip or log. In this dataset, usually a rule exists.
        unmatched_count += 1
        # print(f"No match for tx: {row['psp_reference']}") # Debug

print(f"\nProcessing Complete.")
print(f"Matched Transactions: {matched_count}")
print(f"Unmatched Transactions: {unmatched_count}")
print(f"Total Fees Paid by {merchant_name} in October: €{total_fees:.2f}")

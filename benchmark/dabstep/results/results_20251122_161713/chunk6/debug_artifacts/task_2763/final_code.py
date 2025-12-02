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
        except ValueError:
            return 0.0
    return float(value) if value is not None else 0.0

def parse_range_value(val_str):
    """Parses range strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if not isinstance(val_str, str):
        return None, None
    
    s = val_str.strip()
    
    # Handle k/m suffixes
    def parse_num(n_str):
        n_str = n_str.lower().replace('%', '')
        mult = 1
        if 'k' in n_str:
            mult = 1000
            n_str = n_str.replace('k', '')
        elif 'm' in n_str:
            mult = 1000000
            n_str = n_str.replace('m', '')
        try:
            val = float(n_str)
            if '%' in val_str: # Original string had %
                val /= 100
            return val * mult
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_num(parts[0]), parse_num(parts[1])
    elif s.startswith('>'):
        return parse_num(s[1:]), float('inf')
    elif s.startswith('<'):
        return float('-inf'), parse_num(s[1:])
    else:
        # Exact match treated as range [x, x]
        v = parse_num(s)
        return v, v

def check_overlap(val, rule_range_str):
    """Checks if a value falls within the rule's range string."""
    if rule_range_str is None:
        return True
    
    # Special handling for capture_delay strings
    if isinstance(val, str) and val in ['immediate', 'manual']:
        if rule_range_str == val:
            return True
        # If rule is inequality, we need to convert 'immediate' to 0
        if val == 'immediate':
            num_val = 0
        else:
            return False # 'manual' only matches 'manual' or null
    else:
        try:
            num_val = float(val)
        except:
            return False

    low, high = parse_range_value(rule_range_str)
    
    if rule_range_str.startswith('>'):
        return num_val > low
    if rule_range_str.startswith('<'):
        return num_val < high
    
    return low <= num_val <= high

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Manual: "fee = fixed_amount + rate * transaction_value / 10000"
    return fixed + (rate * amount / 10000.0)

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    tx_ctx: dict containing transaction details and merchant stats
    rule: dict from fees.json
    """
    # 1. Card Scheme (Already filtered in main loop, but good to check)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List) - Empty list means ANY
    if rule.get('account_type') and len(rule['account_type']) > 0:
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List) - Empty list means ANY
    if rule.get('merchant_category_code') and len(rule['merchant_category_code']) > 0:
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Capture Delay (String/Range)
    if rule.get('capture_delay'):
        if not check_overlap(tx_ctx['capture_delay'], rule['capture_delay']):
            return False

    # 5. Monthly Volume (Range)
    if rule.get('monthly_volume'):
        if not check_overlap(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 6. Monthly Fraud Level (Range)
    if rule.get('monthly_fraud_level'):
        if not check_overlap(tx_ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    # 7. Is Credit (Bool)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 8. ACI (List) - Empty list means ANY
    if rule.get('aci') and len(rule['aci']) > 0:
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Bool/Float)
    if rule.get('intracountry') is not None:
        # Rule might use 0.0/1.0 or False/True
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False

    return True

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Martinis 2023
merchant_name = 'Martinis_Fine_Steakhouse'
df = payments[(payments['merchant'] == merchant_name) & (payments['year'] == 2023)].copy()

# 3. Get Merchant Metadata
m_meta = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not m_meta:
    raise ValueError("Merchant not found")

account_type = m_meta['account_type']
mcc = m_meta['merchant_category_code']
capture_delay = m_meta['capture_delay']

# 4. Calculate Monthly Stats
# Map day_of_year to month
def get_month(doy):
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cum_days = 0
    for i, days in enumerate(days_in_months):
        cum_days += days
        if doy <= cum_days:
            return i + 1
    return 12

df['month'] = df['day_of_year'].apply(get_month)

monthly_stats = {}
for month in range(1, 13):
    m_df = df[df['month'] == month]
    vol = m_df['eur_amount'].sum()
    fraud_vol = m_df[m_df['has_fraudulent_dispute']]['eur_amount'].sum()
    # Fraud level: ratio of fraudulent volume over total volume
    fraud_rate = (fraud_vol / vol) if vol > 0 else 0.0
    monthly_stats[month] = {'vol': vol, 'fraud_rate': fraud_rate}

# 5. Pre-calculate transaction contexts
transactions = []
for _, row in df.iterrows():
    intra = (row['issuing_country'] == row['acquirer_country'])
    transactions.append({
        'eur_amount': row['eur_amount'],
        'month': row['month'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': intra
    })

# 6. Simulate Fees for Each Scheme
schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
scheme_totals = {}

for scheme in schemes:
    # Filter rules for this scheme
    scheme_rules = [r for r in fees if r['card_scheme'] == scheme]
    # Sort rules by ID to ensure deterministic order
    scheme_rules.sort(key=lambda x: x['ID'])
    
    total_fees = 0.0
    
    for tx in transactions:
        # Build full context
        m_stats = monthly_stats[tx['month']]
        ctx = {
            'card_scheme': scheme,
            'account_type': account_type,
            'mcc': mcc,
            'capture_delay': capture_delay,
            'monthly_volume': m_stats['vol'],
            'monthly_fraud_level': m_stats['fraud_rate'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': tx['intracountry']
        }
        
        # Find first matching rule
        matched_rule = None
        for rule in scheme_rules:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fees += fee
        else:
            # If no rule matches, assume 0 fee (or could be an error in real scenario)
            pass
            
    scheme_totals[scheme] = total_fees

# 7. Find Max and Output
max_scheme = max(scheme_totals, key=scheme_totals.get)
print(max_scheme)
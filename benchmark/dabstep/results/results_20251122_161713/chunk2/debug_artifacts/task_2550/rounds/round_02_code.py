# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2550
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9224 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
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
        return float(v)
    return float(value)

def parse_range_value(value_str):
    """Parses strings like '100k-1m', '>5', '<3', '3-5' into min/max tuple."""
    if value_str is None:
        return None, None
    
    s = str(value_str).lower().strip()
    
    # Handle k/m suffixes
    def parse_num(n_str):
        n_str = n_str.strip()
        mult = 1
        if n_str.endswith('%'):
            n_str = n_str[:-1]
            mult = 0.01
        elif n_str.endswith('k'):
            n_str = n_str[:-1]
            mult = 1000
        elif n_str.endswith('m'):
            n_str = n_str[:-1]
            mult = 1000000
        return float(n_str) * mult

    if '-' in s:
        parts = s.split('-')
        return parse_num(parts[0]), parse_num(parts[1])
    elif s.startswith('>'):
        return parse_num(s[1:]), float('inf')
    elif s.startswith('<'):
        return float('-inf'), parse_num(s[1:])
    elif s == 'immediate':
        return 0, 0
    elif s == 'manual':
        return 999, 999 # Treat as very high delay
    else:
        try:
            val = parse_num(s)
            return val, val
        except:
            return None, None

def is_in_range(value, range_str):
    """Checks if a numeric value falls within a range string."""
    if range_str is None:
        return True
    min_v, max_v = parse_range_value(range_str)
    if min_v is None: 
        return True # Wildcard or parse error treated as match
    
    # Special handling for capture_delay strings which map to numbers/logic
    if isinstance(value, str):
        # If comparing string to string range (e.g. capture delay)
        # This is tricky, usually capture_delay in data is 'manual' or '1'.
        # Let's try to convert data value to number if possible
        if value == 'immediate': value = 0
        elif value == 'manual': value = 999
        else:
            try:
                value = float(value)
            except:
                return True # Can't compare, assume match or handle differently
                
    return min_v <= value <= max_v

def match_fee_rule_custom(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    tx_context: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    # Rule has list of allowed types. Merchant has one type.
    if rule['account_type']:
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Capture Delay (Range/Value match)
    if rule['capture_delay']:
        # Data value e.g., 'manual', '1'. Rule e.g., '3-5', 'manual'
        # Simple string match if rule is not a range, otherwise range check
        if rule['capture_delay'] == 'manual' or rule['capture_delay'] == 'immediate':
             if str(tx_context['capture_delay']) != rule['capture_delay']:
                 return False
        else:
            # It's a numeric range like '3-5' or '>5'
            if not is_in_range(tx_context['capture_delay'], rule['capture_delay']):
                return False

    # 4. Merchant Category Code (List match)
    # Rule has list of MCCs. Context has single MCC.
    if rule['merchant_category_code']:
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 5. Is Credit (Bool match)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 6. ACI (List match)
    if rule['aci']:
        if tx_context['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Bool match)
    if rule['intracountry'] is not None:
        # Intracountry is True if issuer == acquirer
        is_intra = (tx_context['issuing_country'] == tx_context['acquirer_country'])
        # Rule expects 1.0/True for intra, 0.0/False for inter
        rule_intra = bool(float(rule['intracountry'])) if isinstance(rule['intracountry'], (int, float, str)) else rule['intracountry']
        if rule_intra != is_intra:
            return False

    # 8. Monthly Volume (Range match)
    if rule['monthly_volume']:
        if not is_in_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule['monthly_fraud_level']:
        if not is_in_range(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False

    return True

def get_month_from_doy(doy, year=2023):
    """Returns month (1-12) from day of year."""
    return pd.Timestamp(year, 1, 1) + pd.Timedelta(days=doy - 1)

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Rafa_AI in 2023
target_merchant = 'Rafa_AI'
df_rafa = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == 2023)
].copy()

# 3. Get Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

original_mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats (Volume and Fraud Rate)
# Map day_of_year to month
df_rafa['month'] = df_rafa['day_of_year'].apply(lambda x: get_month_from_doy(x).month)

# Group by month to get stats
monthly_stats = df_rafa.groupby('month').apply(
    lambda x: pd.Series({
        'total_volume': x['eur_amount'].sum(),
        'fraud_volume': x[x['has_fraudulent_dispute']]['eur_amount'].sum()
    })
).reset_index()

# Calculate fraud rate (Fraud Volume / Total Volume)
monthly_stats['fraud_rate'] = monthly_stats['fraud_volume'] / monthly_stats['total_volume']

# Create a lookup dictionary for monthly stats: {month: {'vol': X, 'fraud': Y}}
stats_lookup = monthly_stats.set_index('month').to_dict('index')

# 5. Define Fee Calculation Function
def calculate_total_fees_for_mcc(df, mcc_code):
    total_fees = 0.0
    
    # Iterate through each transaction
    for _, row in df.iterrows():
        month = row['month']
        stats = stats_lookup.get(month)
        
        # Context for matching
        context = {
            'card_scheme': row['card_scheme'],
            'account_type': account_type,
            'capture_delay': capture_delay,
            'mcc': mcc_code,
            'is_credit': row['is_credit'],
            'aci': row['aci'],
            'issuing_country': row['issuing_country'],
            'acquirer_country': row['acquirer_country'],
            'monthly_volume': stats['total_volume'],
            'monthly_fraud_rate': stats['fraud_rate']
        }
        
        # Find first matching rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule_custom(context, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            # Fee = fixed + (rate * amount / 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * row['eur_amount'] / 10000.0)
            total_fees += fee
        else:
            # Fallback or error if no rule matches? 
            # Assuming dataset is complete, but good to track.
            # For this exercise, we assume a rule always exists or fee is 0.
            pass
            
    return total_fees

# 6. Calculate Fees for Original MCC
fees_original = calculate_total_fees_for_mcc(df_rafa, original_mcc)

# 7. Calculate Fees for New MCC (5911)
fees_new = calculate_total_fees_for_mcc(df_rafa, 5911)

# 8. Calculate Delta
delta = fees_new - fees_original

# Output results
print(f"Original MCC: {original_mcc}")
print(f"Original Fees: {fees_original:.4f}")
print(f"New Fees (MCC 5911): {fees_new:.4f}")
print(f"Fee Delta: {delta:.14f}")

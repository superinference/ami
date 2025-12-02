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

def parse_range(range_str):
    """Parses range strings like '100k-1m', '>5', '<3', '7.7%-8.3%'."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower()
    
    def parse_val(v):
        v = v.replace('%', '')
        mult = 1
        if 'k' in v:
            mult = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1000000
            v = v.replace('m', '')
        return float(v) * mult

    is_pct = '%' in s

    if '-' in s:
        parts = s.split('-')
        low = parse_val(parts[0])
        high = parse_val(parts[1])
        if is_pct:
            low /= 100
            high /= 100
        return low, high
    elif '>' in s:
        val = parse_val(s.replace('>', ''))
        if is_pct: val /= 100
        return val, float('inf')
    elif '<' in s:
        val = parse_val(s.replace('<', ''))
        if is_pct: val /= 100
        return float('-inf'), val
    
    return None, None

def check_range(value, range_str):
    """Checks if value falls into the range string."""
    if range_str is None:
        return True
    low, high = parse_range(range_str)
    if low is None: 
        return False 
    return low <= value <= high

def match_fee_rule(tx_ctx, rule):
    """Matches a transaction context against a fee rule."""
    # 1. Card Scheme
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (Wildcard allowed)
    if rule['account_type']: 
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Capture Delay (Wildcard allowed)
    if rule['capture_delay']:
        r_cd = str(rule['capture_delay'])
        t_cd = str(tx_ctx['capture_delay'])
        if r_cd == t_cd:
            pass
        elif t_cd.isdigit() and ('>' in r_cd or '<' in r_cd or '-' in r_cd):
            days = float(t_cd)
            low, high = parse_range(r_cd)
            if not (low <= days <= high):
                return False
        else:
            return False

    # 4. Monthly Fraud Level (Range)
    if rule['monthly_fraud_level']:
        if not check_range(tx_ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    # 5. Monthly Volume (Range)
    if rule['monthly_volume']:
        if not check_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 6. Merchant Category Code (List)
    if rule['merchant_category_code']:
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 7. Is Credit (Bool/Null)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 8. ACI (List)
    if rule['aci']:
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Bool/Null)
    if rule['intracountry'] is not None:
        r_intra = rule['intracountry']
        # Handle string "0.0"/"1.0" or float 0.0/1.0
        if isinstance(r_intra, str):
            r_bool = (float(r_intra) == 1.0)
        else:
            r_bool = bool(r_intra)
            
        if r_bool != tx_ctx['intracountry']:
            return False

    return True

def calculate_fee(amount, rule):
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# --- Main Execution ---

# 1. Load Data
payments = pd.read_csv('/output/chunk2/data/context/payments.csv')
with open('/output/chunk2/data/context/fees.json') as f:
    fees = json.load(f)
with open('/output/chunk2/data/context/merchant_data.json') as f:
    merchant_data = json.load(f)

# 2. Filter for Merchant and December
merchant_name = 'Golfclub_Baron_Friso'
# December 2023 starts on day 335
df = payments[(payments['merchant'] == merchant_name) & (payments['day_of_year'] >= 335)].copy()

# 3. Calculate Monthly Stats (Volume & Fraud)
# These determine the fee tier for ALL transactions in the month
total_vol = df['eur_amount'].sum()
fraud_vol = df[df['has_fraudulent_dispute']]['eur_amount'].sum()
fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0

# 4. Get Merchant Profile
m_profile = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not m_profile:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

# 5. Calculate Fees for each transaction
calculated_fees = []

for idx, row in df.iterrows():
    # Build Context for Matching
    ctx = {
        'card_scheme': row['card_scheme'],
        'account_type': m_profile['account_type'],
        'capture_delay': m_profile['capture_delay'],
        'monthly_fraud_level': fraud_rate,
        'monthly_volume': total_vol,
        'merchant_category_code': m_profile['merchant_category_code'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': (row['issuing_country'] == row['acquirer_country'])
    }
    
    # Find First Matching Rule
    matched_rule = None
    for rule in fees:
        if match_fee_rule(ctx, rule):
            matched_rule = rule
            break 
            
    if matched_rule:
        fee = calculate_fee(row['eur_amount'], matched_rule)
        calculated_fees.append({
            'card_scheme': row['card_scheme'],
            'fee': fee,
            'amount': row['eur_amount']
        })

# 6. Analyze Results
results_df = pd.DataFrame(calculated_fees)

if not results_df.empty:
    # To "pay the maximum fees", we look for the scheme with the highest Average Fee per Transaction.
    # This represents the most expensive scheme to route a transaction through.
    avg_fees = results_df.groupby('card_scheme')['fee'].mean()
    
    # Identify the most expensive scheme
    most_expensive_scheme = avg_fees.idxmax()
    
    print(most_expensive_scheme)
else:
    print("No applicable fees found.")
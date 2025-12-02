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

def parse_range_check(value, range_str):
    """Checks if value is within range_str (e.g. '100k-1m', '>5', '<3', '7.7%-8.3%')."""
    if range_str is None:
        return True
    
    s = str(range_str).strip().lower()
    is_percent = '%' in s
    
    def parse_num(n_str):
        n_str = n_str.replace('%', '')
        if 'k' in n_str:
            return float(n_str.replace('k', '')) * 1000
        if 'm' in n_str:
            return float(n_str.replace('m', '')) * 1000000
        try:
            return float(n_str)
        except:
            return 0.0

    check_val = float(value)
    
    try:
        if '>' in s:
            limit = parse_num(s.replace('>', '').replace('=', ''))
            if is_percent:
                # Normalize limit to ratio if it looks like percentage (e.g. 8.3 -> 0.083)
                limit = limit / 100 if limit > 1 else limit
                # Normalize check_val to ratio if it looks like percentage (e.g. 8.3 -> 0.083)
                if check_val > 1: check_val /= 100
            return check_val > limit 
        elif '<' in s:
            limit = parse_num(s.replace('<', '').replace('=', ''))
            if is_percent:
                limit = limit / 100 if limit > 1 else limit
                if check_val > 1: check_val /= 100
            return check_val < limit
        elif '-' in s:
            parts = s.split('-')
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            if is_percent:
                low = low / 100 if low > 1 else low
                high = high / 100 if high > 1 else high
                if check_val > 1: check_val /= 100
            return low <= check_val <= high
        elif s == 'immediate':
            return str(value) == 'immediate'
        elif s == 'manual':
            return str(value) == 'manual'
        else:
            return str(value) == s
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    # 1. Card Scheme
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List)
    if rule['account_type'] and tx_ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. Capture Delay
    if rule['capture_delay'] is not None:
        m_delay = tx_ctx['capture_delay']
        r_delay = rule['capture_delay']
        # If merchant has numeric delay (e.g. "1") and rule is range
        if str(m_delay).replace('.','',1).isdigit() and any(x in str(r_delay) for x in ['<','>','-']):
             if not parse_range_check(m_delay, r_delay):
                 return False
        elif str(m_delay) != str(r_delay):
             return False

    # 4. Monthly Fraud Level
    if rule['monthly_fraud_level'] is not None:
        if not parse_range_check(tx_ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    # 5. Monthly Volume
    if rule['monthly_volume'] is not None:
        if not parse_range_check(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 6. Merchant Category Code (List)
    if rule['merchant_category_code'] and tx_ctx['mcc'] not in rule['merchant_category_code']:
        return False

    # 7. Is Credit (Bool/None)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 8. ACI (List)
    if rule['aci'] and tx_ctx['aci'] not in rule['aci']:
        return False

    # 9. Intracountry (Bool/None)
    if rule['intracountry'] is not None:
        required_intra = bool(rule['intracountry'])
        if required_intra != tx_ctx['intracountry']:
            return False

    return True

def calculate_fee(amount, rule):
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# --- Main Script ---
payments_path = '/output/chunk5/data/context/payments.csv'
merchant_path = '/output/chunk5/data/context/merchant_data.json'
fees_path = '/output/chunk5/data/context/fees.json'

df = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)

# Filter for Belles_cookbook_store and July (Day 182-212)
merchant_name = 'Belles_cookbook_store'
july_start = 182
july_end = 212

# Get Merchant Info
m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not m_info:
    print("Merchant not found")
    exit()

# Calculate Monthly Stats for July
july_txs = df[
    (df['merchant'] == merchant_name) & 
    (df['day_of_year'] >= july_start) & 
    (df['day_of_year'] <= july_end)
]

monthly_vol = july_txs['eur_amount'].sum()
fraud_vol = july_txs[july_txs['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_ratio = fraud_vol / monthly_vol if monthly_vol > 0 else 0

# Identify Fraudulent Transactions to "Move"
target_txs = july_txs[july_txs['has_fraudulent_dispute'] == True].copy()

# Iterate ACIs
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
results = {}

for aci in possible_acis:
    total_fee = 0
    
    for _, tx in target_txs.iterrows():
        is_intra = (tx['issuing_country'] == tx['acquirer_country'])
        
        ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': m_info['account_type'],
            'capture_delay': m_info['capture_delay'],
            'monthly_fraud_level': monthly_fraud_ratio,
            'monthly_volume': monthly_vol,
            'mcc': m_info['merchant_category_code'],
            'is_credit': bool(tx['is_credit']),
            'aci': aci,
            'intracountry': is_intra
        }
        
        # Find Rule
        matched_rule = None
        for rule in fees:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break 
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fee += fee
        else:
            # If no rule matches, we assume standard fee or skip. 
            # For this problem, we assume coverage exists.
            pass

    results[aci] = total_fee

# Find Best
best_aci = min(results, key=results.get)
print(best_aci)
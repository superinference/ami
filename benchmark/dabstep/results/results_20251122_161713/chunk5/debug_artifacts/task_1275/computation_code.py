import pandas as pd
import json
import numpy as np
import datetime

# --- Helper Functions ---

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
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def is_not_empty(obj):
    """Checks if a list/array is not None and not empty."""
    if obj is None:
        return False
    if isinstance(obj, (list, tuple, np.ndarray)):
        return len(obj) > 0
    return False

def parse_rule_range(range_str, is_percentage=False):
    """Parses range strings from fees.json (e.g., '100k-1m', '>8.3%') into (min, max) tuples."""
    if range_str is None:
        return None
    
    s = str(range_str).strip().lower().replace(',', '')
    
    # Handle percentage conversion if flagged
    scale = 0.01 if is_percentage and '%' in s else 1.0
    s = s.replace('%', '')
    
    # Handle k/m suffixes for volume
    def parse_val(x):
        m = 1
        if x.endswith('k'): m = 1000; x = x[:-1]
        elif x.endswith('m'): m = 1000000; x = x[:-1]
        try:
            return float(x) * m * scale
        except ValueError:
            return 0.0

    try:
        if '-' in s:
            parts = s.split('-')
            return (parse_val(parts[0]), parse_val(parts[1]))
        elif s.startswith('>'):
            return (parse_val(s[1:]), float('inf'))
        elif s.startswith('<'):
            return (float('-inf'), parse_val(s[1:]))
        else:
            # Exact match treated as range [x, x]
            v = parse_val(s)
            return (v, v)
    except:
        return None

def check_rule_match(tx, rule):
    """Checks if a transaction matches a specific fee rule."""
    
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx['card_scheme']:
        return False
    
    # 2. Is Credit
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx['is_credit']:
            return False
            
    # 3. Account Type (List match - wildcard if empty)
    if is_not_empty(rule.get('account_type')):
        if tx['account_type'] not in rule['account_type']:
            return False
            
    # 4. Merchant Category Code (List match - wildcard if empty)
    if is_not_empty(rule.get('merchant_category_code')):
        if tx['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 5. ACI (List match - wildcard if empty)
    if is_not_empty(rule.get('aci')):
        if tx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean match - wildcard if None)
    if rule.get('intracountry') is not None:
        # fees.json uses 0.0/1.0 or null for boolean
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx['intracountry']:
            return False
            
    # 7. Capture Delay (String or Range match)
    if rule.get('capture_delay'):
        r_delay = str(rule['capture_delay']).lower()
        t_delay = str(tx['capture_delay']).lower()
        
        if r_delay == t_delay:
            pass # Exact match
        elif r_delay in ['manual', 'immediate'] or t_delay in ['manual', 'immediate']:
            # If one is keyword and they don't match, fail
            if r_delay != t_delay: return False
        else:
            # Numeric range check (e.g. tx="1" vs rule="<3")
            try:
                t_val = float(t_delay)
                rng = parse_rule_range(r_delay)
                if rng and not (rng[0] <= t_val <= rng[1]):
                    return False
            except:
                if r_delay != t_delay: return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        rng = parse_rule_range(rule['monthly_volume'])
        if rng and not (rng[0] <= tx['monthly_volume'] <= rng[1]):
            return False
            
    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        # Rule is e.g. ">8.3%" -> parsed to (0.083, inf)
        rng = parse_rule_range(rule['monthly_fraud_level'], is_percentage=True)
        if rng and not (rng[0] <= tx['monthly_fraud_ratio'] <= rng[1]):
            return False
            
    return True

# --- Main Execution ---

# 1. Load Data
payments = pd.read_csv('/output/chunk5/data/context/payments.csv')
with open('/output/chunk5/data/context/merchant_data.json') as f:
    merchant_data = json.load(f)
with open('/output/chunk5/data/context/fees.json') as f:
    fees = json.load(f)

# 2. Prepare Merchant Data
merch_df = pd.DataFrame(merchant_data)
# Ensure merge keys match
merch_df = merch_df[['merchant', 'account_type', 'merchant_category_code', 'capture_delay']]

# 3. Calculate Monthly Stats (Volume & Fraud) on FULL dataset
# Convert day_of_year to month (Year is 2023)
# Using a fixed year 2023 for date conversion
payments['date'] = pd.to_datetime(payments['year'].astype(str) + '-01-01') + pd.to_timedelta(payments['day_of_year'] - 1, unit='D')
payments['month'] = payments['date'].dt.month

# Group by merchant and month
monthly_stats = payments.groupby(['merchant', 'month']).agg(
    monthly_volume=('eur_amount', 'sum'),
    fraud_volume=('eur_amount', lambda x: x[payments.loc[x.index, 'has_fraudulent_dispute']].sum())
).reset_index()

# Calculate fraud ratio (fraud volume / total volume)
monthly_stats['monthly_fraud_ratio'] = monthly_stats['fraud_volume'] / monthly_stats['monthly_volume']
monthly_stats['monthly_fraud_ratio'] = monthly_stats['monthly_fraud_ratio'].fillna(0.0)

# 4. Filter Target Transactions (SwiftCharge + Credit)
target_txs = payments[
    (payments['card_scheme'] == 'SwiftCharge') & 
    (payments['is_credit'] == True)
].copy()

# 5. Enrich Target Transactions
# Merge merchant static data
target_txs = target_txs.merge(merch_df, on='merchant', how='left')
# Merge monthly dynamic stats
target_txs = target_txs.merge(monthly_stats[['merchant', 'month', 'monthly_volume', 'monthly_fraud_ratio']], on=['merchant', 'month'], how='left')
# Calculate Intracountry (True if issuing == acquirer)
target_txs['intracountry'] = target_txs['issuing_country'] == target_txs['acquirer_country']

# 6. Filter Relevant Fees (Optimization)
# We only care about SwiftCharge and Credit rules (or wildcards)
relevant_fees = [
    f for f in fees 
    if (f.get('card_scheme') == 'SwiftCharge' or f.get('card_scheme') is None) and
       (f.get('is_credit') is True or f.get('is_credit') is None)
]

# 7. Calculate Fees for 10 EUR
calculated_fees = []
transaction_value = 10.0
tx_records = target_txs.to_dict('records')

for tx in tx_records:
    matched_fee = None
    # Find first matching rule (fees.json order matters)
    for rule in relevant_fees:
        if check_rule_match(tx, rule):
            matched_fee = rule
            break 
            
    if matched_fee:
        # Fee Formula: fixed + (rate * amount / 10000)
        # Note: rate is an integer (basis points equivalent), so divide by 10000
        fee = matched_fee['fixed_amount'] + (matched_fee['rate'] * transaction_value / 10000)
        calculated_fees.append(fee)
    else:
        # Fallback if no rule matches (should not happen with complete rules, but good for debug)
        # Assuming 0 or logging error. For this exercise, we skip or assume 0.
        pass

# 8. Output Result
if calculated_fees:
    avg_fee = sum(calculated_fees) / len(calculated_fees)
    # Print with high precision as requested by patterns
    print(f"{avg_fee:.14f}")
else:
    print("0.00000000000000")
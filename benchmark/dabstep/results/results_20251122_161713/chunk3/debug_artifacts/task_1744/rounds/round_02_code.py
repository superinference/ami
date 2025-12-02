# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1744
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7905 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

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
    """
    Checks if a numeric value falls within a range string.
    Range string examples: '100k-1m', '>5', '<3', '0%-0.5%', '7.7%-8.3%'
    """
    if range_str is None:
        return True
    
    try:
        # Handle k/m suffixes for volume
        def parse_val(s):
            s = s.strip()
            factor = 1
            if s.endswith('%'):
                s = s[:-1]
                factor = 0.01
            elif s.lower().endswith('k'):
                s = s[:-1]
                factor = 1000
            elif s.lower().endswith('m'):
                s = s[:-1]
                factor = 1000000
            return float(s) * factor

        if '-' in range_str:
            low_str, high_str = range_str.split('-')
            low = parse_val(low_str)
            high = parse_val(high_str)
            return low <= value <= high
        elif range_str.startswith('>'):
            limit = parse_val(range_str[1:])
            return value > limit
        elif range_str.startswith('<'):
            limit = parse_val(range_str[1:])
            return value < limit
        else:
            # Exact match or single value (unlikely for ranges but possible)
            return value == parse_val(range_str)
    except:
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_context must contain:
    - card_scheme, is_credit, aci, intracountry (from transaction)
    - account_type, merchant_category_code, capture_delay (from merchant data)
    - monthly_volume, monthly_fraud_rate (calculated stats)
    """
    
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List containment or Wildcard)
    if rule['account_type'] and tx_context['account_type'] not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List containment or Wildcard)
    if rule['merchant_category_code'] and tx_context['merchant_category_code'] not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (Exact match or Wildcard)
    # Note: Rule specifies required delay. Merchant must match.
    if rule['capture_delay'] and rule['capture_delay'] != tx_context['capture_delay']:
        return False
        
    # 5. Is Credit (Boolean match or Wildcard)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_context['is_credit']:
        return False
        
    # 6. ACI (List containment or Wildcard)
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False
        
    # 7. Intracountry (Boolean match or Wildcard)
    # Note: fees.json uses 0.0/1.0 or null. tx_context uses boolean.
    if rule['intracountry'] is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    # 8. Monthly Volume (Range check)
    if rule['monthly_volume'] and not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume']):
        return False
        
    # 9. Monthly Fraud Level (Range check)
    if rule['monthly_fraud_level'] and not parse_range_check(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
        return False
        
    return True

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

# File paths
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)

# 2. Filter for Merchant and Year
target_merchant = "Martinis_Fine_Steakhouse"
target_year = 2023

df_merchant_txs = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

if df_merchant_txs.empty:
    print("No transactions found for this merchant in 2023.")
    exit()

# 3. Get Merchant Static Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Merchant {target_merchant} not found in merchant_data.json")
    exit()

# 4. Calculate Monthly Stats (Volume and Fraud Rate)
# Add month column (assuming data is 2023, we can use day_of_year to approximate or just assume month logic if date existed)
# Since we only have day_of_year, we map it to month.
def get_month(day_of_year):
    # Simple approximation for 2023 (non-leap)
    cumulative_days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]
    for i, d in enumerate(cumulative_days):
        if day_of_year <= d:
            return i # Returns month 1-12
    return 12

df_merchant_txs['month'] = df_merchant_txs['day_of_year'].apply(get_month)

# Group by month to get stats
monthly_stats = df_merchant_txs.groupby('month').agg(
    total_volume=('eur_amount', 'sum'),
    fraud_volume=('eur_amount', lambda x: x[df_merchant_txs.loc[x.index, 'has_fraudulent_dispute']].sum())
).reset_index()

monthly_stats['fraud_rate'] = monthly_stats['fraud_volume'] / monthly_stats['total_volume'] # Ratio (0.0 - 1.0)

# 5. Identify Applicable Fees
applicable_fee_ids = set()

# Optimization: Instead of iterating every row, iterate unique combinations per month
# Relevant columns for fee matching: card_scheme, is_credit, aci, intracountry
# Plus the monthly stats which depend on 'month'

# Determine intracountry for all rows
df_merchant_txs['intracountry'] = df_merchant_txs['issuing_country'] == df_merchant_txs['acquirer_country']

# Group by relevant columns to reduce iterations
grouped = df_merchant_txs.groupby(['month', 'card_scheme', 'is_credit', 'aci', 'intracountry']).size().reset_index(name='count')

for _, row in grouped.iterrows():
    month = row['month']
    
    # Get monthly stats
    stats = monthly_stats[monthly_stats['month'] == month].iloc[0]
    monthly_vol = stats['total_volume']
    monthly_fraud = stats['fraud_rate']
    
    # Build context
    tx_context = {
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['intracountry'],
        'account_type': merchant_info['account_type'],
        'merchant_category_code': merchant_info['merchant_category_code'],
        'capture_delay': merchant_info['capture_delay'],
        'monthly_volume': monthly_vol,
        'monthly_fraud_rate': monthly_fraud
    }
    
    # Check against all rules
    for rule in fees:
        if match_fee_rule(tx_context, rule):
            applicable_fee_ids.add(rule['ID'])

# 6. Output Result
sorted_ids = sorted(list(applicable_fee_ids))
print(", ".join(map(str, sorted_ids)))

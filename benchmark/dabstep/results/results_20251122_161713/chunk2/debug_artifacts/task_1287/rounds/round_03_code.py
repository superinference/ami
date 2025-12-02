# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1287
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 8906 characters (FULL CODE)
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

def parse_range(range_str):
    """Parses strings like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle percentages
    is_pct = '%' in s
    if is_pct:
        s = s.replace('%', '')
        scale = 0.01
    else:
        scale = 1.0
        
    # Handle k/m suffixes for volume
    def parse_val(v):
        v = v.strip()
        mult = 1
        if 'k' in v:
            mult = 1000
            v = v.replace('k', '')
        elif 'm' in v:
            mult = 1000000
            v = v.replace('m', '')
        try:
            return float(v) * mult * scale
        except ValueError:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('<'):
        return 0, parse_val(s[1:])
    elif s.startswith('>'):
        return parse_val(s[1:]), float('inf')
    else:
        # Exact value or malformed
        try:
            val = parse_val(s)
            return val, val
        except:
            return None, None

def check_range(value, range_str):
    """Checks if a value falls within a range string."""
    if range_str is None:
        return True
    min_v, max_v = parse_range(range_str)
    if min_v is None: 
        return True 
    return min_v <= value <= max_v

def match_fee_rule(tx_context, rule):
    """Checks if a transaction context matches a fee rule."""
    # 1. Card Scheme
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Credit/Debit (Rule can be True, False, or None/Null)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_context['is_credit']:
        return False
        
    # 3. Intracountry (Rule can be 0.0, 1.0, or None)
    if rule['intracountry'] is not None:
        # Convert rule value to boolean for comparison
        # 0.0 -> False, 1.0 -> True
        rule_intra = str(rule['intracountry']).lower() in ['true', '1', '1.0']
        if rule_intra != tx_context['intracountry']:
            return False

    # 4. ACI (List in rule, single value in tx)
    if rule['aci'] is not None:
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 5. Account Type (List in rule, single value in tx)
    if rule['account_type'] is not None and len(rule['account_type']) > 0:
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 6. Merchant Category Code (List in rule, single value in tx)
    if rule['merchant_category_code'] is not None and len(rule['merchant_category_code']) > 0:
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 7. Capture Delay (String match or Range)
    if rule['capture_delay'] is not None:
        r_delay = str(rule['capture_delay'])
        m_delay = str(tx_context['capture_delay'])
        
        # Direct match (handles 'immediate', 'manual' if exact)
        if r_delay == m_delay:
            pass
        # Range match (handles <3, >5, 3-5)
        elif any(x in r_delay for x in ['<', '>', '-']):
            # Convert merchant delay to number
            m_val = None
            if m_delay.isdigit():
                m_val = float(m_delay)
            elif m_delay == 'immediate':
                m_val = 0.0 # Treat immediate as 0 days
            
            if m_val is not None:
                min_d, max_d = parse_range(r_delay)
                if not (min_d <= m_val <= max_d):
                    return False
            else:
                # Merchant is 'manual' or unknown, but rule is numeric range -> Mismatch
                return False
        else:
            return False

    # 8. Monthly Volume (Range check)
    if rule['monthly_volume'] is not None:
        if not check_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 9. Monthly Fraud Level (Range check)
    if rule['monthly_fraud_level'] is not None:
        if not check_range(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Calculate Monthly Stats (Volume & Fraud)
# Convert day_of_year to month to calculate monthly stats
# Assuming non-leap year 2023
df_payments['date'] = pd.to_datetime(df_payments['year'] * 1000 + df_payments['day_of_year'], format='%Y%j')
df_payments['month'] = df_payments['date'].dt.month

# Group by merchant and month
# Calculate total volume and fraud volume
monthly_stats = df_payments.groupby(['merchant', 'month']).apply(
    lambda x: pd.Series({
        'volume': x['eur_amount'].sum(),
        'fraud_vol': x[x['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    })
).reset_index()

# Calculate fraud rate (Fraud Volume / Total Volume)
monthly_stats['fraud_rate'] = monthly_stats['fraud_vol'] / monthly_stats['volume']
monthly_stats['fraud_rate'] = monthly_stats['fraud_rate'].fillna(0.0)

# Create lookup dictionary: (merchant, month) -> {volume, fraud_rate}
stats_lookup = {}
for _, row in monthly_stats.iterrows():
    stats_lookup[(row['merchant'], row['month'])] = {
        'volume': row['volume'],
        'fraud_rate': row['fraud_rate']
    }

# 3. Filter Target Transactions
# SwiftCharge + Credit
target_txs = df_payments[
    (df_payments['card_scheme'] == 'SwiftCharge') & 
    (df_payments['is_credit'] == True)
].copy()

# 4. Enrich and Calculate Fees
# Create merchant info lookup
merchant_lookup = {m['merchant']: m for m in merchant_data}

total_weighted_fee = 0
total_count = 0
transaction_value = 500.0

# Optimization: Group by unique contexts to reduce iterations
# Context keys: merchant, month, aci, issuing_country, acquirer_country
grouped_txs = target_txs.groupby(['merchant', 'month', 'aci', 'issuing_country', 'acquirer_country']).size().reset_index(name='count')

for _, row in grouped_txs.iterrows():
    merchant_name = row['merchant']
    month = row['month']
    aci = row['aci']
    issuing = row['issuing_country']
    acquirer = row['acquirer_country']
    count = row['count']
    
    # Get Merchant Static Data
    m_info = merchant_lookup.get(merchant_name)
    if not m_info:
        continue
        
    # Get Merchant Dynamic Stats (for that month)
    m_stats = stats_lookup.get((merchant_name, month))
    if not m_stats:
        continue
        
    # Determine Intracountry
    is_intracountry = (issuing == acquirer)
    
    # Build Context for Rule Matching
    context = {
        'card_scheme': 'SwiftCharge',
        'is_credit': True,
        'aci': aci,
        'intracountry': is_intracountry,
        'merchant_category_code': m_info['merchant_category_code'],
        'account_type': m_info['account_type'],
        'capture_delay': m_info['capture_delay'],
        'monthly_volume': m_stats['volume'],
        'monthly_fraud_level': m_stats['fraud_rate']
    }
    
    # Find first matching rule
    matched_rule = None
    for rule in fees:
        if match_fee_rule(context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Calculate fee for 500 EUR
        fee = calculate_fee(transaction_value, matched_rule)
        total_weighted_fee += fee * count
        total_count += count

# 5. Output Result
if total_count > 0:
    avg_fee = total_weighted_fee / total_count
    print(f"{avg_fee:.14f}")
else:
    print("No applicable transactions found.")

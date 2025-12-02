# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2485
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9767 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np
from datetime import datetime

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
                # For ranges, we can't return a single float for comparison unless specified
                # But for parsing "100k-1m", we handle in parse_range
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range_value(val_str):
    """Parses strings like '100k', '1m', '8.3%' into floats."""
    if not isinstance(val_str, str): return val_str
    s = val_str.lower().replace(',', '').replace('%', '').replace('€', '')
    factor = 1
    if '%' in val_str: factor = 0.01
    
    if 'k' in s:
        return float(s.replace('k', '')) * 1000
    if 'm' in s:
        return float(s.replace('m', '')) * 1000000
    try:
        return float(s) * factor
    except:
        return 0.0

def check_range_match(value, range_str):
    """Checks if a numeric value fits within a string range (e.g., '100k-1m', '<3', '>5')."""
    if range_str is None:
        return True
    
    s = str(range_str).lower().strip()
    
    # Handle inequalities
    if s.startswith('<'):
        limit = parse_range_value(s[1:])
        return value < limit
    if s.startswith('>'):
        limit = parse_range_value(s[1:])
        return value > limit
        
    # Handle ranges
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = parse_range_value(parts[0])
            max_val = parse_range_value(parts[1])
            return min_val <= value <= max_val
            
    # Handle exact match (rare for these fields but possible)
    try:
        target = parse_range_value(s)
        return value == target
    except:
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_context must contain: 
      card_scheme, account_type, merchant_category_code, is_credit, aci, 
      intracountry, monthly_volume, monthly_fraud_level, capture_delay
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match - wildcard if empty/None)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match - wildcard if empty/None)
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Bool match - wildcard if None)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 5. ACI (List match - wildcard if empty/None)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Bool match - wildcard if None)
    if rule.get('intracountry') is not None:
        if rule['intracountry'] != tx_context['intracountry']:
            return False

    # 7. Capture Delay (String match - wildcard if None)
    if rule.get('capture_delay'):
        # capture_delay in rule is a string like "manual" or range like "3-5"
        # merchant data has specific value. 
        # If rule is a specific value (manual, immediate), exact match.
        # If rule is a range (<3, 3-5), we need to parse merchant value if it's numeric.
        
        r_cd = str(rule['capture_delay'])
        m_cd = str(tx_context['capture_delay'])
        
        if r_cd in ['manual', 'immediate']:
            if r_cd != m_cd: return False
        else:
            # It's a numeric range rule, check if merchant value is numeric
            if m_cd in ['manual', 'immediate']:
                return False # Numeric rule doesn't match string status
            try:
                days = float(m_cd)
                if not check_range_match(days, r_cd):
                    return False
            except:
                return False # Can't compare

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range_match(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_range_match(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# File paths
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Crossfit_Hanna and 2023
target_merchant = 'Crossfit_Hanna'
df_merchant = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == 2023)
].copy()

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 4. Calculate Monthly Stats (Volume and Fraud Rate)
# Convert day_of_year to month
# 2023 is not a leap year
df_merchant['date'] = pd.to_datetime(df_merchant['year'] * 1000 + df_merchant['day_of_year'], format='%Y%j')
df_merchant['month'] = df_merchant['date'].dt.month

monthly_stats = {}
for month in range(1, 13):
    df_m = df_merchant[df_merchant['month'] == month]
    if len(df_m) > 0:
        vol = df_m['eur_amount'].sum()
        fraud_count = df_m['has_fraudulent_dispute'].sum()
        fraud_rate = (fraud_count / vol) if vol > 0 else 0.0 # Fraud level is usually fraud_vol/total_vol or count/count?
        # Manual says: "fraud levels measured as ratio between monthly total volume and monthly volume notified as fraud"
        # Wait, manual says: "ratio between monthly total volume and monthly volume notified as fraud" -> This phrasing is ambiguous.
        # Usually it is (Fraud Volume / Total Volume).
        # Let's check if 'has_fraudulent_dispute' implies the whole amount is fraud. Yes.
        fraud_vol = df_m[df_m['has_fraudulent_dispute']]['eur_amount'].sum()
        fraud_ratio = fraud_vol / vol if vol > 0 else 0.0
        
        monthly_stats[month] = {
            'volume': vol,
            'fraud_ratio': fraud_ratio
        }
    else:
        monthly_stats[month] = {'volume': 0.0, 'fraud_ratio': 0.0}

# 5. Identify Target Fee Rule (ID 276)
target_fee_id = 276
target_fee_rule = next((f for f in fees_data if f['ID'] == target_fee_id), None)

if not target_fee_rule:
    print("Fee ID 276 not found.")
    exit()

old_rate = target_fee_rule['rate']
new_rate = 1
rate_diff = new_rate - old_rate

# 6. Iterate Transactions to Calculate Delta
# We must find transactions where Fee 276 is the *active* fee (first match).
affected_amount_sum = 0.0

# Sort fees by ID (assuming ID indicates priority/order in list)
# Usually fees are applied in order of appearance or specificity. 
# Without specific instructions, list order is the safest assumption for "rule engines".
# fees_data is a list, so it preserves order.
sorted_fees = fees_data 

count_affected = 0

for _, row in df_merchant.iterrows():
    # Build context for this transaction
    month = row['month']
    stats = monthly_stats.get(month, {'volume': 0, 'fraud_ratio': 0})
    
    # Intracountry check
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
    context = {
        'card_scheme': row['card_scheme'],
        'account_type': merchant_info['account_type'],
        'merchant_category_code': merchant_info['merchant_category_code'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': is_intracountry,
        'capture_delay': merchant_info['capture_delay'],
        'monthly_volume': stats['volume'],
        'monthly_fraud_level': stats['fraud_ratio']
    }
    
    # Find the FIRST matching rule
    active_rule_id = None
    for rule in sorted_fees:
        if match_fee_rule(context, rule):
            active_rule_id = rule['ID']
            break
    
    # Check if the active rule is our target rule
    if active_rule_id == target_fee_id:
        affected_amount_sum += row['eur_amount']
        count_affected += 1

# 7. Calculate Delta
# Delta = (New Rate - Old Rate) * Amount / 10000
# We can sum amounts first since (C*A + C*B) = C*(A+B)
total_delta = (rate_diff * affected_amount_sum) / 10000.0

# Output
print(f"Transactions affected: {count_affected}")
print(f"Total affected volume: {affected_amount_sum:.2f}")
print(f"Old Rate: {old_rate}")
print(f"New Rate: {new_rate}")
print(f"Calculated Delta: {total_delta:.14f}")

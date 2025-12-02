# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1748
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 8526 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m suffixes to float."""
    if pd.isna(value) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    
    s = str(value).strip().lower().replace(',', '').replace('€', '').replace('$', '')
    
    # Handle comparison operators for simple parsing if needed
    s = s.lstrip('><≤≥')
    
    try:
        if '%' in s:
            return float(s.replace('%', '')) / 100.0
        if 'k' in s:
            return float(s.replace('k', '')) * 1000
        if 'm' in s:
            return float(s.replace('m', '')) * 1000000
        return float(s)
    except ValueError:
        return 0.0

def parse_range_check(value, range_str):
    """
    Checks if a value falls within a range string (e.g., '100k-1m', '<3', '>5', '0%-1%').
    Returns True if range_str is None (wildcard).
    """
    if range_str is None:
        return True
        
    s = str(range_str).strip().lower().replace(',', '').replace('€', '').replace('$', '')
    
    # Handle exact string matches first (e.g., 'immediate', 'manual')
    if isinstance(value, str) and s == value.lower():
        return True
        
    # If value is string but not exact match, try converting to float if possible
    val_float = value
    if isinstance(value, str):
        try:
            val_float = float(value)
        except ValueError:
            # If value is non-numeric string (e.g. 'manual') and range is numeric (e.g. '<3'), no match
            return False

    def parse_bound(b_str):
        return coerce_to_float(b_str)

    try:
        if '-' in s:
            parts = s.split('-')
            low = parse_bound(parts[0])
            high = parse_bound(parts[1])
            return low <= val_float <= high
        elif s.startswith('<'):
            limit = parse_bound(s[1:])
            return val_float < limit
        elif s.startswith('>'):
            limit = parse_bound(s[1:])
            return val_float > limit
        else:
            # Exact numeric match
            return val_float == parse_bound(s)
    except:
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match - Wildcard allowed)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match - Wildcard allowed)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Complex match - Wildcard allowed)
    if rule.get('capture_delay'):
        # Rule might be '3-5', '<3', 'immediate'. Merchant might be '1', 'immediate'.
        if not parse_range_check(tx_context['capture_delay'], rule['capture_delay']):
            return False

    # 5. Is Credit (Bool match - Wildcard allowed)
    if rule.get('is_credit') is not None:
        # Normalize rule boolean
        r_credit = rule['is_credit']
        if isinstance(r_credit, str):
            r_credit = r_credit.lower() == 'true'
        if r_credit != tx_context['is_credit']:
            return False

    # 6. ACI (List match - Wildcard allowed)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Bool match - Wildcard allowed)
    if rule.get('intracountry') is not None:
        rule_intra = rule['intracountry']
        # Normalize rule value to bool (0.0 -> False, 1.0 -> True, 'True' -> True)
        if isinstance(rule_intra, (float, int)):
            rule_intra = bool(rule_intra)
        elif isinstance(rule_intra, str):
            rule_intra = rule_intra.lower() == 'true'
            
        if rule_intra != tx_context['intracountry']:
            return False

    # 8. Monthly Volume (Range match - Wildcard allowed)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match - Wildcard allowed)
    if rule.get('monthly_fraud_level'):
        # Fraud level in rule is like '0%-0.5%'. 
        # tx_context['monthly_fraud_rate'] is float 0.004.
        # parse_range_check handles the % conversion in the rule string via coerce_to_float
        if not parse_range_check(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    if fixed is None: fixed = 0.0
    
    rate = rule.get('rate', 0.0)
    if rate is None: rate = 0.0
    
    # Rate is basis points / 10000
    variable = (rate * amount) / 10000.0
    return fixed + variable

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data_list = json.load(f)

# 2. Filter for Merchant and Year
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023

df_filtered = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

if df_filtered.empty:
    print(0.0)
    exit()

# 3. Get Merchant Static Data
merchant_info = next((item for item in merchant_data_list if item["merchant"] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

# 4. Pre-calculate Monthly Stats (Volume and Fraud Rate)
# Convert day_of_year to month
# 2023 is not a leap year.
df_filtered['date'] = pd.to_datetime(df_filtered['year'] * 1000 + df_filtered['day_of_year'], format='%Y%j')
df_filtered['month'] = df_filtered['date'].dt.month

# Group by month to get stats
monthly_stats = {}
grouped = df_filtered.groupby('month')

for month, group in grouped:
    total_vol = group['eur_amount'].sum()
    
    # Fraud is defined as ratio of fraudulent VOLUME over total VOLUME (Manual Sec 7)
    fraud_vol = group[group['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate_vol = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'monthly_volume': total_vol,
        'monthly_fraud_rate': fraud_rate_vol
    }

# 5. Calculate Fees
total_fees = 0.0
matched_count = 0
unmatched_count = 0

# Iterate through transactions
for idx, row in df_filtered.iterrows():
    # Build context
    month = row['month']
    stats = monthly_stats.get(month, {'monthly_volume': 0, 'monthly_fraud_rate': 0})
    
    tx_context = {
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['issuing_country'] == row['acquirer_country'],
        'account_type': merchant_info['account_type'],
        'mcc': merchant_info['merchant_category_code'],
        'capture_delay': merchant_info['capture_delay'],
        'monthly_volume': stats['monthly_volume'],
        'monthly_fraud_rate': stats['monthly_fraud_rate']
    }
    
    # Find matching rule
    # We assume the first matching rule in the list applies
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        fee = calculate_fee(row['eur_amount'], matched_rule)
        total_fees += fee
        matched_count += 1
    else:
        unmatched_count += 1
        # If no rule matches, we assume 0 fee or skip. 
        # In a real scenario, this might be an error, but here we sum what we find.

# 6. Output Result
# Round to 2 decimal places as it is currency
print(f"{total_fees:.2f}")

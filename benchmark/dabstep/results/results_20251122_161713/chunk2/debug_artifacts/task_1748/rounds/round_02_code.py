# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1748
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9901 characters (FULL CODE)
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
    
    # Handle comparison operators for simple parsing if needed (though ranges usually handled separately)
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

def parse_range_check(value, range_str, is_percentage=False):
    """
    Checks if a value falls within a range string (e.g., '100k-1m', '<3', '>5').
    Returns True if range_str is None (wildcard).
    """
    if range_str is None:
        return True
        
    # Normalize range string
    s = str(range_str).strip().lower().replace(',', '').replace('€', '').replace('$', '')
    
    # Handle Percentage scaling for the value if the range is percentage
    # The value passed in is likely already a float (e.g. 0.08 for 8%), 
    # but the range might be "5%-10%".
    # We need to align them. 
    # Strategy: Convert range bounds to floats (0.05, 0.10) and compare with value (0.08).
    
    def parse_bound(b_str):
        return coerce_to_float(b_str)

    try:
        if '-' in s:
            parts = s.split('-')
            low = parse_bound(parts[0])
            high = parse_bound(parts[1])
            return low <= value <= high
        elif s.startswith('<'):
            limit = parse_bound(s[1:])
            return value < limit
        elif s.startswith('>'):
            limit = parse_bound(s[1:])
            return value > limit
        else:
            # Exact match or single value (rare for ranges, but possible)
            return value == parse_bound(s)
    except:
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_context must contain:
      - card_scheme, is_credit, aci, intracountry (from transaction)
      - account_type, mcc, capture_delay (from merchant data)
      - monthly_volume, monthly_fraud_rate (calculated stats)
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match - Wildcard allowed)
    # Rule has list of types, merchant has single type.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match - Wildcard allowed)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Exact match - Wildcard allowed)
    if rule.get('capture_delay'):
        # Handle range logic for capture delay if it appears as range, 
        # but usually it's categorical in merchant_data ('immediate', 'manual', etc.)
        # However, fees.json has '3-5', '>5'. merchant_data has '1', 'immediate'.
        # We need to map merchant value to rule logic.
        # Let's try direct match first, then logic.
        r_cd = str(rule['capture_delay'])
        m_cd = str(tx_context['capture_delay'])
        
        if r_cd == m_cd:
            pass # Match
        elif r_cd == 'immediate' and m_cd == 'immediate':
            pass
        elif r_cd == 'manual' and m_cd == 'manual':
            pass
        else:
            # Numeric comparison
            try:
                days = float(m_cd)
                if not parse_range_check(days, r_cd):
                    return False
            except ValueError:
                # If merchant delay is 'immediate'/'manual' but rule is numeric range, or vice versa
                return False

    # 5. Is Credit (Bool match - Wildcard allowed)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 6. ACI (List match - Wildcard allowed)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Bool match - Wildcard allowed)
    # Note: fees.json uses 0.0/1.0 for boolean sometimes, or true/false
    if rule.get('intracountry') is not None:
        rule_intra = rule['intracountry']
        # Normalize rule value to bool
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
        if not parse_range_check(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level'], is_percentage=True):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Rate is an integer to be divided by 10000 (basis points logic often, or specified in manual)
    # Manual says: "Variable rate to be especified to be multiplied by the transaction value and divided by 10000."
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
    fraud_count = group['has_fraudulent_dispute'].sum()
    tx_count = len(group)
    fraud_rate = fraud_count / total_vol if total_vol > 0 else 0.0 # Fraud is ratio of fraudulent VOLUME over total VOLUME (Manual Sec 7)
    # Wait, manual says: "Fraud is defined as the ratio of fraudulent volume over total volume."
    # Let's check Sec 7 again: "Fraud is defined as the ratio of fraudulent volume over total volume."
    # Let's check Sec 5: "monthly_fraud_level: ... ratio between monthly total volume and monthly volume notified as fraud."
    # Okay, so it is Volume based, not Count based.
    
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

# Pre-process fees to avoid repeated lookups if possible, but list is small (1000), so loop is fine.
# We iterate through transactions.

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
    # Assuming the first matching rule applies.
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
        # If no rule matches, fee is 0 or we should flag it. 
        # In this context, we assume 0 or missing data.

# 6. Output Result
# Round to 2 decimal places as it is currency
print(f"{total_fees:.2f}")

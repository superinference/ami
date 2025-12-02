# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1743
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9925 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean for simple coercion, 
        # but specific range parsers should be used for logic
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                # Check if it's a range like "100k-1m"
                if 'k' in v or 'm' in v:
                    return 0.0 # Placeholder, specific parser needed
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        return float(v) if v else 0.0
    return 0.0

def parse_volume_string(vol_str):
    """Parses volume strings like '100k-1m' into (min, max)."""
    if not vol_str:
        return (0, float('inf'))
    
    def parse_val(s):
        s = s.lower().strip()
        mult = 1
        if 'k' in s:
            mult = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mult = 1000000
            s = s.replace('m', '')
        return float(s) * mult

    try:
        if '-' in vol_str:
            parts = vol_str.split('-')
            return (parse_val(parts[0]), parse_val(parts[1]))
        elif '>' in vol_str:
            return (parse_val(vol_str.replace('>', '')), float('inf'))
        elif '<' in vol_str:
            return (0, parse_val(vol_str.replace('<', '')))
        else:
            val = parse_val(vol_str)
            return (val, val)
    except:
        return (0, float('inf'))

def parse_fraud_string(fraud_str):
    """Parses fraud strings like '>8.3%' or '7.7%-8.3%' into (min, max)."""
    if not fraud_str:
        return (0.0, float('inf'))
    
    def parse_pct(s):
        s = s.strip().replace('%', '')
        return float(s) / 100.0

    try:
        if '-' in fraud_str:
            parts = fraud_str.split('-')
            return (parse_pct(parts[0]), parse_pct(parts[1]))
        elif '>' in fraud_str:
            return (parse_pct(fraud_str.replace('>', '')), float('inf'))
        elif '<' in fraud_str:
            return (0.0, parse_pct(fraud_str.replace('<', '')))
        else:
            val = parse_pct(fraud_str)
            return (val, val)
    except:
        return (0.0, float('inf'))

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches the rule."""
    if rule_delay is None:
        return True
    
    # Normalize strings
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    if r_delay == m_delay:
        return True
    
    # Handle numeric comparisons if both look like numbers or ranges
    # Merchant delay might be "1", "7". Rule might be "<3", ">5", "3-5"
    try:
        # If merchant delay is numeric (e.g. "1")
        if m_delay.isdigit():
            m_val = float(m_delay)
            if '-' in r_delay:
                low, high = map(float, r_delay.split('-'))
                return low <= m_val <= high
            elif '>' in r_delay:
                return m_val > float(r_delay.replace('>', ''))
            elif '<' in r_delay:
                return m_val < float(r_delay.replace('<', ''))
    except:
        pass
        
    return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_context must contain: 
      card_scheme, is_credit, aci, intracountry, 
      merchant_mcc, merchant_account_type, merchant_capture_delay,
      monthly_volume, monthly_fraud_rate
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match or Wildcard)
    if rule['account_type'] and tx_context['merchant_account_type'] not in rule['account_type']:
        return False

    # 3. Merchant Category Code (List match or Wildcard)
    if rule['merchant_category_code'] and tx_context['merchant_mcc'] not in rule['merchant_category_code']:
        return False

    # 4. Capture Delay (Complex match or Wildcard)
    if not check_capture_delay(tx_context['merchant_capture_delay'], rule['capture_delay']):
        return False

    # 5. Monthly Volume (Range match or Wildcard)
    if rule['monthly_volume']:
        min_vol, max_vol = parse_volume_string(rule['monthly_volume'])
        if not (min_vol <= tx_context['monthly_volume'] <= max_vol):
            return False

    # 6. Monthly Fraud Level (Range match or Wildcard)
    if rule['monthly_fraud_level']:
        min_fraud, max_fraud = parse_fraud_string(rule['monthly_fraud_level'])
        # Fraud rate in context is 0.0-1.0
        if not (min_fraud <= tx_context['monthly_fraud_rate'] <= max_fraud):
            return False

    # 7. Is Credit (Boolean match or Wildcard)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 8. ACI (List match or Wildcard)
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False

    # 9. Intracountry (Boolean match or Wildcard)
    if rule['intracountry'] is not None:
        # Intracountry in context is 0.0 or 1.0 (float) or boolean
        is_intra = bool(tx_context['intracountry'])
        rule_intra = bool(rule['intracountry'])
        if is_intra != rule_intra:
            return False

    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# File paths
payments_path = '/output/chunk6/data/context/payments.csv'
fees_path = '/output/chunk6/data/context/fees.json'
merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

# 1. Load Data
print("Loading data...")
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_metadata_list = json.load(f)

# 2. Filter for Merchant and Year
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023

print(f"Filtering for {target_merchant} in {target_year}...")
df_merchant = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

if df_merchant.empty:
    print("No transactions found for this merchant in 2023.")
    exit()

# 3. Get Merchant Metadata
merchant_info = next((item for item in merchant_metadata_list if item["merchant"] == target_merchant), None)
if not merchant_info:
    print(f"Metadata for {target_merchant} not found.")
    exit()

print(f"Merchant Metadata: MCC={merchant_info['merchant_category_code']}, "
      f"Account={merchant_info['account_type']}, "
      f"Delay={merchant_info['capture_delay']}")

# 4. Calculate Monthly Stats & Find Applicable Fees
# We group by month because volume/fraud rules apply to monthly aggregates.
# However, we need to collect ALL applicable IDs across the year.

# Add month column (assuming day_of_year is available, approximate month or just group by it if needed)
# Since we don't have explicit month column, we can derive it from day_of_year roughly, 
# or treat the whole dataset as one period if the prompt implies "average monthly".
# Manual says: "Monthly volumes and rates are computed always in natural months".
# We will approximate months from day_of_year (1-365).
df_merchant['month'] = pd.cut(df_merchant['day_of_year'], 
                              bins=[0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 366], 
                              labels=range(1, 13))

applicable_fee_ids = set()

# Iterate through each month present in the data
for month, group in df_merchant.groupby('month', observed=True):
    if group.empty:
        continue
        
    # Calculate Monthly Stats
    total_volume = group['eur_amount'].sum()
    fraud_volume = group[group['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    # Identify unique transaction profiles in this month
    # We only need to check unique combinations of attributes that affect fee rules
    # Attributes: card_scheme, is_credit, aci, intracountry
    
    # Calculate intracountry for each row
    # Local acquiring: issuer country == acquirer country
    group['is_intracountry'] = group['issuing_country'] == group['acquirer_country']
    
    unique_txs = group[['card_scheme', 'is_credit', 'aci', 'is_intracountry']].drop_duplicates()
    
    for _, tx in unique_txs.iterrows():
        # Build context for matching
        context = {
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': tx['is_intracountry'],
            'merchant_mcc': merchant_info['merchant_category_code'],
            'merchant_account_type': merchant_info['account_type'],
            'merchant_capture_delay': merchant_info['capture_delay'],
            'monthly_volume': total_volume,
            'monthly_fraud_rate': fraud_rate
        }
        
        # Check against ALL fee rules
        for rule in fees_data:
            if match_fee_rule(context, rule):
                applicable_fee_ids.add(rule['ID'])

# 5. Output Results
sorted_ids = sorted(list(applicable_fee_ids))
print("\nApplicable Fee IDs for Golfclub_Baron_Friso in 2023:")
print(", ".join(map(str, sorted_ids)))

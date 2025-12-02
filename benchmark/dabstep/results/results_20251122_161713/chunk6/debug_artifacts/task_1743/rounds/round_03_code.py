# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1743
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 9354 characters (FULL CODE)
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
        return float(v) if v else 0.0
    return 0.0

def parse_volume_string(vol_str):
    """Parses volume strings like '100k-1m' into (min, max)."""
    if not vol_str:
        return (0, float('inf'))
    
    def parse_val(s):
        s = str(s).lower().strip()
        mult = 1
        if 'k' in s:
            mult = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mult = 1000000
            s = s.replace('m', '')
        try:
            return float(s) * mult
        except:
            return 0.0

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
        s = str(s).strip().replace('%', '')
        try:
            return float(s) / 100.0
        except:
            return 0.0

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
    m_delay = str(merchant_delay).lower().strip()
    r_delay = str(rule_delay).lower().strip()
    
    # Direct string match (e.g., "manual" == "manual", "immediate" == "immediate")
    if r_delay == m_delay:
        return True
    
    # Handle numeric comparisons
    # Merchant delay might be "1", "7". Rule might be "<3", ">5", "3-5"
    if m_delay.replace('.', '', 1).isdigit():
        m_val = float(m_delay)
        try:
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
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match or Wildcard)
    # Rule account_type is a list. Merchant account_type is a string.
    # If rule list is empty, it matches all.
    if rule.get('account_type'):
        if tx_context['merchant_account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match or Wildcard)
    # Rule MCC is a list. Merchant MCC is an int/string.
    if rule.get('merchant_category_code'):
        # Ensure types match (convert both to int for safety)
        try:
            rule_mccs = [int(x) for x in rule['merchant_category_code']]
            merch_mcc = int(tx_context['merchant_mcc'])
            if merch_mcc not in rule_mccs:
                return False
        except:
            # Fallback if conversion fails
            if tx_context['merchant_mcc'] not in rule['merchant_category_code']:
                return False

    # 4. Capture Delay (Complex match or Wildcard)
    if not check_capture_delay(tx_context['merchant_capture_delay'], rule.get('capture_delay')):
        return False

    # 5. Monthly Volume (Range match or Wildcard)
    if rule.get('monthly_volume'):
        min_vol, max_vol = parse_volume_string(rule['monthly_volume'])
        # Check if volume is within range
        if not (min_vol <= tx_context['monthly_volume'] <= max_vol):
            return False

    # 6. Monthly Fraud Level (Range match or Wildcard)
    if rule.get('monthly_fraud_level'):
        min_fraud, max_fraud = parse_fraud_string(rule['monthly_fraud_level'])
        # Fraud rate in context is 0.0-1.0
        if not (min_fraud <= tx_context['monthly_fraud_rate'] <= max_fraud):
            return False

    # 7. Is Credit (Boolean match or Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 8. ACI (List match or Wildcard)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Boolean match or Wildcard)
    if rule.get('intracountry') is not None:
        # Intracountry in context is boolean
        # Rule might be 0.0/1.0 or boolean
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
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_metadata_list = json.load(f)

# 2. Filter for Merchant and Year
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023

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

# 4. Calculate Monthly Stats & Find Applicable Fees
# Map day_of_year to month (1-12)
# Standard non-leap year mapping (2023 is not a leap year)
# Jan: 1-31, Feb: 32-59, Mar: 60-90, etc.
bins = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 366]
labels = range(1, 13)
df_merchant['month'] = pd.cut(df_merchant['day_of_year'], bins=bins, labels=labels)

applicable_fee_ids = set()

# Iterate through each month present in the data
for month, group in df_merchant.groupby('month', observed=True):
    if group.empty:
        continue
        
    # Calculate Monthly Stats
    total_volume = group['eur_amount'].sum()
    
    # Fraud volume: sum of eur_amount where has_fraudulent_dispute is True
    fraud_volume = group[group['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    
    # Fraud rate: fraud_volume / total_volume
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0
    
    # Calculate intracountry for each row
    # Local acquiring: issuer country == acquirer country
    group['is_intracountry'] = group['issuing_country'] == group['acquirer_country']
    
    # Identify unique transaction profiles in this month
    # We only need to check unique combinations of attributes that affect fee rules
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
print(", ".join(map(str, sorted_ids)))

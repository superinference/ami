# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1741
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8167 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, k, m to float. Handles common formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '').replace('_', '')
        # Handle k/m suffixes
        if v.lower().endswith('k'):
            try:
                return float(v[:-1]) * 1000
            except:
                pass
        if v.lower().endswith('m'):
            try:
                return float(v[:-1]) * 1000000
            except:
                pass
        
        v = v.lstrip('><≤≥')  # Remove comparison operators for parsing
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return value # Return original string if not a number (e.g. "manual")
    return value

def parse_range_check(value, rule_str):
    """
    Checks if a value fits a rule string.
    Handles: '100k-1m', '<3', '>5', 'immediate', 'manual', '7.7%-8.3%'
    """
    if rule_str is None:
        return True # Wildcard matches everything
    if value is None:
        return False # If rule exists but value is missing, no match
    
    # 1. Handle exact string matches (e.g., "manual", "immediate")
    if isinstance(rule_str, str) and rule_str.lower() in ['manual', 'immediate']:
        return str(value).lower() == rule_str.lower()
    
    # 2. Try to convert value to float for numeric comparison
    try:
        val_float = coerce_to_float(value)
        if not isinstance(val_float, float):
            # If coercion returned a string (e.g. "manual"), do string comparison
            return str(value) == str(rule_str)
    except:
        return str(value) == str(rule_str)

    # 3. Handle Operators (<, >)
    if isinstance(rule_str, str):
        if rule_str.startswith('<'):
            limit = coerce_to_float(rule_str[1:])
            return val_float < limit
        if rule_str.startswith('>'):
            limit = coerce_to_float(rule_str[1:])
            return val_float > limit
            
        # 4. Handle Ranges (min-max)
        if '-' in rule_str:
            parts = rule_str.split('-')
            if len(parts) == 2:
                try:
                    min_v = coerce_to_float(parts[0])
                    max_v = coerce_to_float(parts[1])
                    return min_v <= val_float <= max_v
                except:
                    pass
                    
        # 5. Handle Exact Numeric Match in String
        try:
            rule_float = coerce_to_float(rule_str)
            return val_float == rule_float
        except:
            pass

    return str(value) == str(rule_str)

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    """
    # 1. Card Scheme (Exact Match)
    if rule.get('card_scheme') != tx_context.get('card_scheme'):
        return False
        
    # 2. Account Type (List Membership)
    # Rule None/[] matches all. Otherwise, merchant's type must be in list.
    if rule.get('account_type') and tx_context.get('account_type') not in rule['account_type']:
        return False
        
    # 3. Merchant Category Code (List Membership)
    if rule.get('merchant_category_code') and tx_context.get('mcc') not in rule['merchant_category_code']:
        return False
        
    # 4. Capture Delay (Range/Value Check)
    if rule.get('capture_delay'):
        if not parse_range_check(tx_context.get('capture_delay'), rule['capture_delay']):
            return False
            
    # 5. Is Credit (Boolean Match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context.get('is_credit'):
            return False
            
    # 6. ACI (List Membership)
    if rule.get('aci') and tx_context.get('aci') not in rule['aci']:
        return False
        
    # 7. Intracountry (Boolean Match)
    if rule.get('intracountry') is not None:
        # Convert 0.0/1.0/None to boolean
        rule_intra = bool(rule['intracountry'])
        tx_intra = bool(tx_context.get('intracountry'))
        if rule_intra != tx_intra:
            return False
            
    # 8. Monthly Volume (Range Check)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_context.get('monthly_volume'), rule['monthly_volume']):
            return False
            
    # 9. Monthly Fraud Level (Range Check)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_context.get('monthly_fraud_level'), rule['monthly_fraud_level']):
            return False
            
    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk1/data/context/payments.csv'
fees_path = '/output/chunk1/data/context/fees.json'
merchant_data_path = '/output/chunk1/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Target Merchant and Year
target_merchant = 'Belles_cookbook_store'
target_year = 2023

df = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

if df.empty:
    print("No transactions found for this merchant in 2023.")
    exit()

# 3. Get Merchant Static Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Merchant {target_merchant} not found in merchant_data.json")
    exit()

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 4. Pre-calculate Transaction Attributes
# Intracountry: Issuer Country == Acquirer Country
df['intracountry'] = df['issuing_country'] == df['acquirer_country']

# Month: Calculate month from day_of_year (2023 is non-leap)
df['month'] = pd.to_datetime(df['day_of_year'] - 1, unit='D', origin=f'{target_year}-01-01').dt.month

# 5. Calculate Monthly Aggregates (Volume and Fraud Rate)
# Manual: "Monthly volumes and rates are computed always in natural months"
monthly_stats = {}
for month, group in df.groupby('month'):
    total_vol = group['eur_amount'].sum()
    fraud_vol = group[group['has_fraudulent_dispute']]['eur_amount'].sum()
    
    # Fraud Rate = Fraud Volume / Total Volume
    fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    monthly_stats[month] = {
        'volume': total_vol,
        'fraud_rate': fraud_rate
    }

# 6. Find Applicable Fee IDs
# We iterate through unique transaction profiles per month to be efficient
applicable_ids = set()

# Group by attributes that affect fee matching
group_cols = ['month', 'card_scheme', 'is_credit', 'aci', 'intracountry']
unique_profiles = df[group_cols].drop_duplicates()

for _, row in unique_profiles.iterrows():
    month = row['month']
    stats = monthly_stats.get(month)
    
    # Construct the context for this specific transaction profile
    context = {
        'card_scheme': row['card_scheme'],
        'account_type': account_type,
        'mcc': mcc,
        'capture_delay': capture_delay,
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['intracountry'],
        'monthly_volume': stats['volume'],
        'monthly_fraud_level': stats['fraud_rate']
    }
    
    # Check against ALL fee rules
    for rule in fees:
        if match_fee_rule(context, rule):
            applicable_ids.add(rule['ID'])

# 7. Output Result
# Sort IDs numerically
sorted_ids = sorted(list(applicable_ids))
print(", ".join(map(str, sorted_ids)))

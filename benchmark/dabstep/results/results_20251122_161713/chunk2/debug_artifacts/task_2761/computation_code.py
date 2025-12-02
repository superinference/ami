import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float, np.number)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except:
            return 0.0
    return 0.0

def parse_range(range_str):
    """Parses a string range like '100k-1m', '<3', or '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    multiplier = 1
    if 'k' in s: multiplier = 1000
    if 'm' in s: multiplier = 1000000
    
    # Remove units for parsing
    clean_s = s.replace('k', '').replace('m', '').replace('%', '')
    
    is_percentage = '%' in s
    
    if '-' in clean_s:
        parts = clean_s.split('-')
        try:
            v1 = float(parts[0])
            v2 = float(parts[1])
            if is_percentage:
                v1 /= 100
                v2 /= 100
            else:
                v1 *= multiplier
                v2 *= multiplier
            return v1, v2
        except:
            return None, None
    elif '<' in s:
        try:
            val = float(clean_s.replace('<', ''))
            if is_percentage: val /= 100
            else: val *= multiplier
            return float('-inf'), val
        except:
            return None, None
    elif '>' in s:
        try:
            val = float(clean_s.replace('>', ''))
            if is_percentage: val /= 100
            else: val *= multiplier
            return val, float('inf')
        except:
            return None, None
    elif s == 'immediate':
        return 0, 0
    else:
        try:
            val = float(clean_s)
            if is_percentage: val /= 100
            else: val *= multiplier
            return val, val
        except:
            return None, None

def check_overlap(val, range_str):
    """Checks if a numeric value falls within a range string."""
    if range_str is None: return True # Wildcard matches all
    
    # Handle specific keywords
    if range_str == 'immediate' and val == 0: return True
    if range_str == 'manual': return True 
    
    # Parse range
    min_v, max_v = parse_range(range_str)
    if min_v is None: return False 
    
    return min_v <= val <= max_v

# ---------------------------------------------------------
# Main Execution
# ---------------------------------------------------------

# File Paths
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'
fees_path = '/output/chunk2/data/context/fees.json'

# Load Data
df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)

# Target
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023

# 1. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in metadata.")

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 2. Calculate Merchant Statistics (2023)
df_m = df_payments[(df_payments['merchant'] == target_merchant) & (df_payments['year'] == target_year)]

if df_m.empty:
    print("No transactions found")
    exit()

# Calculate Monthly Volume (Total 2023 Volume / 12)
total_volume = df_m['eur_amount'].sum()
monthly_volume = total_volume / 12

# Calculate Fraud Rate (Count of fraud / Total txs)
fraud_count = df_m['has_fraudulent_dispute'].sum()
tx_count = len(df_m)
fraud_rate = fraud_count / tx_count if tx_count > 0 else 0.0

# Capture Delay Numeric for comparison
capture_delay_val = 0
if capture_delay == 'immediate': capture_delay_val = 0
elif capture_delay == 'manual': capture_delay_val = 999
else:
    try:
        capture_delay_val = float(capture_delay)
    except:
        capture_delay_val = 999

# 3. Pre-filter Fees for Merchant Profile (Static Attributes)
# This filters rules based on MCC, Account Type, Volume, Fraud, and Delay
applicable_rules = []
for rule in fees:
    # MCC
    if rule.get('merchant_category_code') and mcc not in rule['merchant_category_code']:
        continue
    # Account Type
    if rule.get('account_type') and account_type not in rule['account_type']:
        continue
    # Monthly Volume
    if rule.get('monthly_volume') and not check_overlap(monthly_volume, rule['monthly_volume']):
        continue
    # Monthly Fraud Level
    if rule.get('monthly_fraud_level') and not check_overlap(fraud_rate, rule['monthly_fraud_level']):
        continue
    # Capture Delay
    if rule.get('capture_delay'):
        if rule['capture_delay'] == 'manual':
            if capture_delay != 'manual': continue
        elif rule['capture_delay'] == 'immediate':
            if capture_delay != 'immediate': continue
        else:
            if not check_overlap(capture_delay_val, rule['capture_delay']):
                continue
    
    applicable_rules.append(rule)

# 4. Simulate Fees for Each Scheme
# We calculate the total fee for the merchant's 2023 transactions AS IF they were processed by each scheme.
schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
scheme_totals = {s: 0.0 for s in schemes}

# Group transactions for efficiency: (is_credit, aci, issuing_country, acquirer_country)
# We need acquirer_country to determine intracountry status
grouped = df_m.groupby(['is_credit', 'aci', 'issuing_country', 'acquirer_country']).agg(
    count=('eur_amount', 'count'),
    total_amount=('eur_amount', 'sum')
).reset_index()

for scheme in schemes:
    scheme_rules = [r for r in applicable_rules if r['card_scheme'] == scheme]
    total_fee = 0.0
    
    for _, row in grouped.iterrows():
        is_credit = row['is_credit']
        aci = row['aci']
        issuing_country = row['issuing_country']
        acquirer_country = row['acquirer_country']
        count = row['count']
        amount_sum = row['total_amount']
        
        # Determine Intracountry
        is_intracountry = (issuing_country == acquirer_country)
        
        # Find best matching rule (highest fee) for this transaction group
        best_fee_for_group = -1.0
        matched = False
        
        for rule in scheme_rules:
            # Check Transaction-Specific Conditions
            
            # is_credit
            if rule.get('is_credit') is not None:
                if rule['is_credit'] != is_credit:
                    continue
            
            # aci
            if rule.get('aci') and aci not in rule['aci']:
                continue
                
            # intracountry
            if rule.get('intracountry') is not None:
                rule_intra = rule['intracountry']
                # Convert JSON values (0.0/1.0) to bool
                if rule_intra == 1.0 or rule_intra is True: rule_intra_bool = True
                elif rule_intra == 0.0 or rule_intra is False: rule_intra_bool = False
                else: rule_intra_bool = None
                
                if rule_intra_bool is not None and rule_intra_bool != is_intracountry:
                    continue
            
            # Rule matches
            matched = True
            # Calculate total fee for this group using this rule
            # Fee = (Fixed * Count) + (Rate * TotalAmount / 10000)
            current_fee = (rule['fixed_amount'] * count) + (rule['rate'] * amount_sum / 10000)
            
            if current_fee > best_fee_for_group:
                best_fee_for_group = current_fee
        
        if matched:
            total_fee += best_fee_for_group
    
    scheme_totals[scheme] = total_fee

# 5. Determine Winner
sorted_schemes = sorted(scheme_totals.items(), key=lambda x: x[1], reverse=True)

if sorted_schemes:
    # Print only the name of the scheme with the highest fees
    print(sorted_schemes[0][0])
else:
    print("No applicable schemes found")
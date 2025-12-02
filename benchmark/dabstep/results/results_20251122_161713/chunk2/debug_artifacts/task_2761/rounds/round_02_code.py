# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2761
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7233 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# Helper Functions
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
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        return float(v)
    return float(value)

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
    
    if '-' in clean_s:
        parts = clean_s.split('-')
        try:
            return float(parts[0]) * multiplier, float(parts[1]) * multiplier
        except:
            return None, None
    elif '<' in s: # Use original s to detect operator
        try:
            val = float(clean_s.replace('<', '')) * multiplier
            return float('-inf'), val
        except:
            return None, None
    elif '>' in s:
        try:
            val = float(clean_s.replace('>', '')) * multiplier
            return val, float('inf')
        except:
            return None, None
    elif s == 'immediate':
        return 0, 0
    else:
        try:
            val = float(clean_s) * multiplier
            return val, val
        except:
            return None, None

def check_overlap(val, range_str, is_percentage=False):
    """Checks if a numeric value falls within a range string."""
    if range_str is None: return True # Wildcard matches all
    
    # Handle specific keywords
    if range_str == 'immediate' and val == 0: return True
    if range_str == 'manual': return True # Assuming manual matches manual context
    
    # Parse range
    min_v, max_v = parse_range(range_str)
    if min_v is None: return False 
    
    # Adjust value for percentage comparison if the range was a percentage
    check_val = val
    if is_percentage and '%' in range_str:
        # Example: range "8%", val 0.08. parse_range returns 8. check_val should be 8.
        check_val = val * 100
        
    return min_v <= check_val <= max_v

def match_fee_rule(merchant_ctx, rule):
    """Determines if a fee rule applies to a merchant's fixed attributes."""
    
    # 1. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        # If rule has MCC list, merchant's MCC must be in it
        if merchant_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 2. Account Type (List match)
    if rule.get('account_type'):
        # If rule has Account Type list, merchant's type must be in it
        if merchant_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_overlap(merchant_ctx['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 4. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_overlap(merchant_ctx['fraud_rate'], rule['monthly_fraud_level'], is_percentage=True):
            return False
            
    # 5. Capture Delay (Range/Exact match)
    if rule.get('capture_delay'):
        m_delay = merchant_ctx['capture_delay']
        # Convert merchant delay to number if possible for comparison
        try:
            m_delay_num = float(m_delay)
        except:
            m_delay_num = 0 if m_delay == 'immediate' else 999 # 999 for manual
            
        if not check_overlap(m_delay_num, rule['capture_delay']):
            return False

    return True

# ---------------------------------------------------------
# Main Execution
# ---------------------------------------------------------

# File Paths
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'
fees_path = '/output/chunk2/data/context/fees.json'
acquirer_path = '/output/chunk2/data/context/acquirer_countries.csv'

# Load Data
df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees = json.load(f)
df_acquirer = pd.read_csv(acquirer_path)

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
# Filter payments
df_m = df_payments[(df_payments['merchant'] == target_merchant) & (df_payments['year'] == target_year)]

# Calculate Average Amount
avg_amount = df_m['eur_amount'].mean()

# Calculate Monthly Volume (Total 2023 Volume / 12)
total_volume = df_m['eur_amount'].sum()
monthly_volume = total_volume / 12

# Calculate Fraud Rate (Count of fraud / Total txs)
fraud_count = df_m['has_fraudulent_dispute'].sum()
tx_count = len(df_m)
fraud_rate = fraud_count / tx_count if tx_count > 0 else 0.0

# Context for matching rules
merchant_ctx = {
    'mcc': mcc,
    'account_type': account_type,
    'monthly_volume': monthly_volume,
    'fraud_rate': fraud_rate,
    'capture_delay': capture_delay
}

# 3. Evaluate Fees per Scheme
max_fees_by_scheme = {}

for rule in fees:
    # Check if rule applies to the merchant's fixed profile
    if match_fee_rule(merchant_ctx, rule):
        scheme = rule['card_scheme']
        
        # Calculate Fee for this rule
        # Fee = Fixed + (Rate * Amount / 10000)
        fixed = rule['fixed_amount']
        rate = rule['rate']
        fee = fixed + (rate * avg_amount / 10000)
        
        # We want the MAXIMUM fee possible for this scheme.
        # Since we are "steering traffic", we assume we might hit the most expensive 
        # transaction type (Credit/Debit, Domestic/Intl) allowed by the scheme's rules.
        if scheme not in max_fees_by_scheme:
            max_fees_by_scheme[scheme] = 0.0
        
        if fee > max_fees_by_scheme[scheme]:
            max_fees_by_scheme[scheme] = fee

# 4. Determine Winner
# Sort by fee descending
sorted_schemes = sorted(max_fees_by_scheme.items(), key=lambda x: x[1], reverse=True)

if sorted_schemes:
    winner_scheme = sorted_schemes[0][0]
    print(winner_scheme)
else:
    print("No applicable schemes found")

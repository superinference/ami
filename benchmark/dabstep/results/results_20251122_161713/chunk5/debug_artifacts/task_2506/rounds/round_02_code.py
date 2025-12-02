# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2506
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7773 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        return float(v)
    return float(value)

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '0.0%-0.5%' into (min, max) tuple."""
    if not isinstance(range_str, str):
        return None, None
    
    # Handle suffixes
    def clean_val(x):
        x = x.lower().strip()
        mult = 1
        if 'k' in x:
            mult = 1000
            x = x.replace('k', '')
        elif 'm' in x:
            mult = 1000000
            x = x.replace('m', '')
        
        # Handle percentages
        if '%' in x:
            return coerce_to_float(x) # coerce handles the /100
            
        return float(x) * mult

    try:
        if '-' in range_str:
            parts = range_str.split('-')
            return clean_val(parts[0]), clean_val(parts[1])
        elif '>' in range_str:
            return clean_val(range_str.replace('>', '')), float('inf')
        elif '<' in range_str:
            return float('-inf'), clean_val(range_str.replace('<', ''))
    except:
        return None, None
    return None, None

def check_range(value, range_str):
    """Checks if a value falls within a string range."""
    if range_str is None:
        return True
    min_v, max_v = parse_range(range_str)
    if min_v is None: # Parsing failed or simple equality (though unlikely for vol/fraud)
        return True
    return min_v <= value <= max_v

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_context must contain: 
      - card_scheme, is_credit, aci, intracountry (tx specific)
      - mcc, account_type, capture_delay (merchant specific)
      - monthly_volume, monthly_fraud_rate (calculated stats)
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 3. Account Type (List match)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 4. Capture Delay (Exact match)
    if rule.get('capture_delay') and rule['capture_delay'] != tx_context['capture_delay']:
        return False

    # 5. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 6. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Intracountry means Issuer Country == Acquirer Country
        if rule['intracountry'] != tx_context['intracountry']:
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False

    return True

# ==========================================
# MAIN SCRIPT
# ==========================================

# File paths
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'

# Load data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)

# Target Merchant and Year
TARGET_MERCHANT = 'Martinis_Fine_Steakhouse'
TARGET_YEAR = 2023
TARGET_FEE_ID = 16
NEW_RATE = 99

# 1. Get Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == TARGET_MERCHANT), None)
if not merchant_info:
    raise ValueError(f"Merchant {TARGET_MERCHANT} not found in merchant_data.json")

m_mcc = merchant_info['merchant_category_code']
m_account_type = merchant_info['account_type']
m_capture_delay = merchant_info['capture_delay']

print(f"Merchant: {TARGET_MERCHANT}")
print(f"Attributes: MCC={m_mcc}, Type={m_account_type}, Delay={m_capture_delay}")

# 2. Get Fee Rule ID=16
fee_rule_16 = next((f for f in fees_data if f['ID'] == TARGET_FEE_ID), None)
if not fee_rule_16:
    raise ValueError(f"Fee ID {TARGET_FEE_ID} not found in fees.json")

old_rate = fee_rule_16['rate']
print(f"Fee ID {TARGET_FEE_ID} found. Old Rate: {old_rate}, New Rate: {NEW_RATE}")
print(f"Rule Criteria: {json.dumps(fee_rule_16, indent=2)}")

# 3. Filter Transactions
df_tx = df_payments[
    (df_payments['merchant'] == TARGET_MERCHANT) & 
    (df_payments['year'] == TARGET_YEAR)
].copy()

print(f"Total transactions for merchant in {TARGET_YEAR}: {len(df_tx)}")

# 4. Calculate Monthly Stats (Volume and Fraud Rate)
# Group by month to calculate stats required for rule matching
df_tx['month'] = pd.to_datetime(df_tx['day_of_year'], unit='D', origin=f'{TARGET_YEAR}-01-01').dt.month

monthly_stats = {}
for month in df_tx['month'].unique():
    month_txs = df_tx[df_tx['month'] == month]
    
    # Volume in Euros
    vol = month_txs['eur_amount'].sum()
    
    # Fraud Rate (Ratio)
    fraud_count = month_txs['has_fraudulent_dispute'].sum()
    total_count = len(month_txs)
    fraud_rate = fraud_count / total_count if total_count > 0 else 0.0
    
    monthly_stats[month] = {
        'volume': vol,
        'fraud_rate': fraud_rate
    }

# 5. Identify Matching Transactions and Calculate Delta
affected_volume = 0.0
matching_tx_count = 0

for idx, row in df_tx.iterrows():
    # Build context for this transaction
    month = row['month']
    
    # Determine intracountry (Issuer == Acquirer)
    # Note: fees.json uses boolean or 1.0/0.0. Helper handles comparison if types align.
    # We convert to boolean for consistency with typical JSON boolean fields.
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
    tx_context = {
        'card_scheme': row['card_scheme'],
        'mcc': m_mcc,
        'account_type': m_account_type,
        'capture_delay': m_capture_delay,
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': is_intracountry,
        'monthly_volume': monthly_stats[month]['volume'],
        'monthly_fraud_rate': monthly_stats[month]['fraud_rate']
    }
    
    # Check if Fee ID 16 applies
    if match_fee_rule(tx_context, fee_rule_16):
        affected_volume += row['eur_amount']
        matching_tx_count += 1

# 6. Calculate Delta
# Formula: Fee = Fixed + (Rate * Amount / 10000)
# Delta = New_Fee - Old_Fee = (New_Rate - Old_Rate) * Amount / 10000
rate_diff = NEW_RATE - old_rate
total_delta = (rate_diff * affected_volume) / 10000

print(f"\nMatching Transactions: {matching_tx_count}")
print(f"Affected Volume: {affected_volume:.2f}")
print(f"Rate Difference: {rate_diff}")
print(f"Calculated Delta: {total_delta:.14f}")

# Final Answer Output
print(f"{total_delta:.14f}")

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1840
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8415 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value == '':
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
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_range(range_str):
    """Parses a range string like '100k-1m', '<5%', '>10' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower().replace(',', '').replace('%', '')
    multiplier = 1
    if 'k' in s:
        multiplier = 1000
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000
        s = s.replace('m', '')
        
    try:
        if '-' in s:
            parts = s.split('-')
            return float(parts[0]) * multiplier, float(parts[1]) * multiplier
        elif s.startswith('>'):
            return float(s[1:]) * multiplier, float('inf')
        elif s.startswith('<'):
            return float('-inf'), float(s[1:]) * multiplier
    except:
        pass
    return None, None

def is_in_range(value, range_str):
    """Checks if a value fits within a range string."""
    if range_str is None:
        return True # Wildcard matches all
    
    # Handle percentage strings in value (e.g. if value is passed as 0.08 for 8%)
    # The range_str might be "7.7%-8.3%"
    # We need to normalize. 
    # If range_str has %, we expect value to be a ratio (0-1) or percentage (0-100).
    # Based on manual, fraud is ratio. Let's standardize on converting range to float.
    
    is_percentage = '%' in range_str
    
    low, high = parse_range(range_str)
    if low is None: 
        return True
        
    # Adjust value if it's a ratio (e.g. 0.08) and range was percentage (e.g. 8)
    check_val = value
    if is_percentage and value < 1.0 and (low > 1.0 or high > 1.0):
        check_val = value * 100
        
    return low <= check_val <= high

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    tx_ctx: dict containing transaction details and monthly stats
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    # If rule has list, tx value must be in it. Empty/Null rule matches all.
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Boolean match)
    # If rule is not None, must match.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Intracountry means Issuer Country == Acquirer Country
        is_intra = (tx_ctx['issuing_country'] == tx_ctx['acquirer_country'])
        # Rule expects boolean or 0.0/1.0
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 7. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not is_in_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        # Fraud level in context is a ratio (e.g. 0.05 for 5%)
        # Range string might be "0%-5%"
        if not is_in_range(tx_ctx['monthly_fraud_rate'] * 100, rule['monthly_fraud_level']):
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate (basis points)."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0)
    # Formula from manual: fee = fixed_amount + rate * transaction_value / 10000
    return fixed + (rate * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
base_path = '/output/chunk4/data/context/'
df_payments = pd.read_csv(base_path + 'payments.csv')
with open(base_path + 'merchant_data.json', 'r') as f:
    merchant_data = json.load(f)
with open(base_path + 'fees.json', 'r') as f:
    fees_data = json.load(f)

# 2. Define Context
target_merchant = 'Golfclub_Baron_Friso'
start_day = 152  # June 1st
end_day = 181    # June 30th

# 3. Get Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']

# 4. Calculate Monthly Stats (Volume & Fraud) for June
# Filter for the specific merchant and month (June)
# Manual says: "Monthly volumes and rates are computed always in natural months"
df_june = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['day_of_year'] >= start_day) & 
    (df_payments['day_of_year'] <= end_day)
].copy()

if len(df_june) == 0:
    print(f"No transactions found for {target_merchant} in June.")
    exit()

monthly_volume = df_june['eur_amount'].sum()
fraud_count = df_june['has_fraudulent_dispute'].sum()
total_count = len(df_june)
monthly_fraud_rate = fraud_count / total_count if total_count > 0 else 0.0

print(f"Merchant: {target_merchant}")
print(f"June Volume: €{monthly_volume:,.2f}")
print(f"June Fraud Rate: {monthly_fraud_rate:.2%}")
print(f"MCC: {mcc}, Account Type: {account_type}")

# 5. Calculate Fees for Each Transaction
total_fees = 0.0
matched_count = 0
unmatched_count = 0

# Pre-sort fees by ID to ensure deterministic matching order (though usually first match in list is implied)
# The problem doesn't specify priority, but usually specific rules come before general ones.
# Assuming the JSON order is the priority order.
sorted_fees = fees_data # sorted(fees_data, key=lambda x: x['ID'])

for _, tx in df_june.iterrows():
    # Build transaction context
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'mcc': mcc,
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'issuing_country': tx['issuing_country'],
        'acquirer_country': tx['acquirer_country'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Find matching rule
    match = None
    for rule in sorted_fees:
        if match_fee_rule(tx_ctx, rule):
            match = rule
            break
            
    if match:
        fee = calculate_fee(tx['eur_amount'], match)
        total_fees += fee
        matched_count += 1
    else:
        # If no rule matches, this is an issue, but we'll log it.
        # In a real scenario, there might be a default fee, but here we assume coverage.
        unmatched_count += 1
        # print(f"Unmatched TX: {tx['psp_reference']}")

# 6. Output Result
print(f"\nTotal Transactions Processed: {len(df_june)}")
print(f"Matched: {matched_count}, Unmatched: {unmatched_count}")
print(f"Total Fees: {total_fees:.2f}")

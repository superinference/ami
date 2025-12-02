import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return None
    if isinstance(value, (int, float, np.number)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        # Handle comparison operators for parsing (stripping them to get the number)
        v_clean = v.lstrip('><≤≥')
        try:
            if '%' in v_clean:
                return float(v_clean.replace('%', '')) / 100
            if 'k' in v_clean.lower():
                return float(v_clean.lower().replace('k', '')) * 1000
            if 'm' in v_clean.lower():
                return float(v_clean.lower().replace('m', '')) * 1000000
            return float(v_clean)
        except ValueError:
            return None
    return None

def check_range(value, rule_string):
    """Check if a value fits within a rule string (e.g., '100k-1m', '>5', '3-5', '>8.3%')."""
    if rule_string is None:
        return True
    
    try:
        # Handle simple equality for non-numeric strings (e.g. 'manual', 'immediate')
        if isinstance(value, str) and not any(c in rule_string for c in ['-', '>', '<', '%', 'k', 'm']):
             return value.lower() == rule_string.lower()

        val_float = coerce_to_float(value)
        
        # If value is not numeric (and rule implies numeric), return False
        if val_float is None:
            # Special case: if rule is numeric but value is string 'manual' etc, it's a mismatch
            return False

        # Handle ranges "min-max"
        if '-' in rule_string:
            parts = rule_string.split('-')
            if len(parts) == 2:
                min_val = coerce_to_float(parts[0])
                max_val = coerce_to_float(parts[1])
                if min_val is not None and max_val is not None:
                    return min_val <= val_float <= max_val
        
        # Handle inequalities
        if rule_string.startswith('>'):
            limit = coerce_to_float(rule_string)
            if limit is not None:
                return val_float > limit
        if rule_string.startswith('<'):
            limit = coerce_to_float(rule_string)
            if limit is not None:
                return val_float < limit
            
        # Handle exact numeric match
        rule_float = coerce_to_float(rule_string)
        if rule_float is not None:
            return val_float == rule_float
            
        return False
    except Exception:
        return False

def match_fee_rule(tx_context, rule):
    """
    Check if a fee rule applies to a transaction context.
    """
    
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match or wildcard [])
    # Rule matches if list is empty OR if merchant's account type is IN the list
    if rule.get('account_type'): # If list is not empty
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match or wildcard [])
    if rule.get('merchant_category_code'): # If list is not empty
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Value/Range match or wildcard null)
    if rule.get('capture_delay') is not None:
        if not check_range(tx_context['capture_delay'], rule['capture_delay']):
            return False
            
    # 5. Monthly Volume (Range match or wildcard null)
    if rule.get('monthly_volume') is not None:
        if not check_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 6. Monthly Fraud Level (Range match or wildcard null)
    if rule.get('monthly_fraud_level') is not None:
        if not check_range(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False
            
    # 7. Is Credit (Boolean match or wildcard null)
    if rule.get('is_credit') is not None:
        # JSON bools are True/False/null. 
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 8. ACI (List match or wildcard [])
    if rule.get('aci'): # If list is not empty
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry (Boolean match or wildcard null)
    if rule.get('intracountry') is not None:
        # rule['intracountry'] might be 0.0, 1.0, or None in JSON which loads as float/None
        # Convert to bool for comparison
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculate fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# File paths
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Get Merchant Metadata
target_merchant = "Crossfit_Hanna"
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)

if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found.")
    exit()

# 3. Calculate Monthly Stats for July 2023
# Day 200 is in July. July is days 182-212 (inclusive) in 2023.
july_start = 182
july_end = 212

july_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == 2023) &
    (df_payments['day_of_year'] >= july_start) &
    (df_payments['day_of_year'] <= july_end)
]

monthly_volume = july_txs['eur_amount'].sum()
fraud_volume = july_txs[july_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 4. Filter Transactions for Day 200
target_day = 200
day_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == 2023) &
    (df_payments['day_of_year'] == target_day)
]

# 5. Calculate Fees
total_fees = 0.0
matched_count = 0
unmatched_count = 0

for _, tx in day_txs.iterrows():
    # Determine intracountry status
    # Local acquiring: issuer country == acquirer country
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    # Build context for matching
    tx_context = {
        'card_scheme': tx['card_scheme'],
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': is_intracountry,
        'account_type': merchant_info['account_type'],
        'mcc': merchant_info['merchant_category_code'],
        'capture_delay': merchant_info['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Find matching rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            matched_rule = rule
            break # Stop at first match
            
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee
        matched_count += 1
    else:
        unmatched_count += 1

# 6. Output Result
# High precision output as requested for financial calculations
print(f"{total_fees:.14f}")
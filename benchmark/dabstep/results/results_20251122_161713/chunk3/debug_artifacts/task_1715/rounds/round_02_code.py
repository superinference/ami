# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1715
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8566 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        # Handle k/m suffixes for volume
        if v.lower().endswith('k'):
            v = v[:-1]
            multiplier = 1000
        elif v.lower().endswith('m'):
            v = v[:-1]
            multiplier = 1000000
        else:
            multiplier = 1
            
        v = v.lstrip('><≤≥')  # Remove comparison operators
        
        if '%' in v:
            return float(v.replace('%', '')) / 100
        
        # Range handling (e.g., "50-60") - return mean for simple coercion, 
        # but for range checking we usually parse bounds separately.
        # This function is for single value conversion.
        try:
            return float(v) * multiplier
        except ValueError:
            return 0.0
    return 0.0

def parse_range_check(value, rule_str):
    """
    Checks if a numeric value fits within a rule string range.
    Handles: "100k-1m", ">5", "<3", "7.7%-8.3%", "immediate", "manual"
    """
    if rule_str is None:
        return True
        
    # Handle special string keywords for capture_delay
    if isinstance(value, str) and isinstance(rule_str, str):
        if rule_str == value:
            return True
        # If merchant is "1" (str) and rule is "<3", we need to convert merchant to float
        try:
            val_float = float(value)
        except ValueError:
            # If merchant value is "manual" or "immediate" and didn't match exact rule above
            return False
    elif isinstance(value, (int, float)):
        val_float = float(value)
    else:
        return False

    # Clean rule string
    s = str(rule_str).strip().lower()
    
    # Handle inequalities
    if s.startswith('>'):
        limit = coerce_to_float(s[1:])
        return val_float > limit
    if s.startswith('<'):
        limit = coerce_to_float(s[1:])
        return val_float < limit
        
    # Handle ranges "min-max"
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return min_val <= val_float <= max_val
            
    # Handle exact numeric match (rare in these rules but possible)
    try:
        return val_float == coerce_to_float(s)
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_ctx: dict containing transaction and merchant details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    # Rule list empty/null means wildcard (applies to all)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Exact match, null=wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Exact match, null=wildcard)
    if rule.get('intracountry') is not None:
        # Convert boolean to float 0.0/1.0 if needed, or compare bools
        # JSON often has 0.0/1.0 for bools
        rule_intra = bool(rule['intracountry'])
        tx_intra = bool(tx_ctx['intracountry'])
        if rule_intra != tx_intra:
            return False
            
    # 7. Capture Delay (Complex match)
    if rule.get('capture_delay'):
        if not parse_range_check(tx_ctx['capture_delay'], rule['capture_delay']):
            return False
            
    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Rate is typically "per 10000" (basis points * 100 or similar, manual says "divided by 10000")
    variable = (rate * amount) / 10000
    return fixed + variable

# ═══════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_path = '/output/chunk3/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Merchant Context
target_merchant = 'Belles_cookbook_store'
merchant_profile = next((m for m in merchant_data if m['merchant'] == target_merchant), None)

if not merchant_profile:
    print(f"Error: Merchant {target_merchant} not found.")
else:
    # Extract static merchant fields
    mcc = merchant_profile['merchant_category_code']
    account_type = merchant_profile['account_type']
    capture_delay = merchant_profile['capture_delay']

    # 3. Calculate Monthly Stats (October 2023)
    # October is days 274 to 304 (inclusive) in a non-leap year like 2023
    oct_start = 274
    oct_end = 304
    
    df_oct = df_payments[
        (df_payments['merchant'] == target_merchant) &
        (df_payments['year'] == 2023) &
        (df_payments['day_of_year'] >= oct_start) &
        (df_payments['day_of_year'] <= oct_end)
    ]
    
    monthly_volume = df_oct['eur_amount'].sum()
    
    # Fraud calculation: Ratio of Fraud Volume / Total Volume
    fraud_txs = df_oct[df_oct['has_fraudulent_dispute'] == True]
    fraud_volume = fraud_txs['eur_amount'].sum()
    
    if monthly_volume > 0:
        monthly_fraud_rate = fraud_volume / monthly_volume
    else:
        monthly_fraud_rate = 0.0
        
    # print(f"DEBUG: Oct Volume: {monthly_volume}, Fraud Rate: {monthly_fraud_rate:.4%}")

    # 4. Filter Target Transactions (Day 300)
    target_day = 300
    df_target = df_payments[
        (df_payments['merchant'] == target_merchant) &
        (df_payments['year'] == 2023) &
        (df_payments['day_of_year'] == target_day)
    ]
    
    # 5. Calculate Fees
    total_fees = 0.0
    
    # Sort fees by ID to ensure deterministic matching order (though usually first match applies)
    fees_data.sort(key=lambda x: x['ID'])
    
    for _, tx in df_target.iterrows():
        # Build transaction context
        tx_ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': account_type,
            'mcc': mcc,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': tx['issuing_country'] == tx['acquirer_country'],
            'capture_delay': capture_delay,
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate
        }
        
        # Find matching rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(tx_ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fees += fee
        else:
            # Fallback or error logging if no rule matches (should not happen in well-formed data)
            # print(f"Warning: No fee rule found for tx {tx['psp_reference']}")
            pass

    # 6. Output Result
    # Question asks for total fees in euros.
    print(f"{total_fees:.2f}")

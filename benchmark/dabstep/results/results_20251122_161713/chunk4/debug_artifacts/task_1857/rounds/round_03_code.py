# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1857
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 8253 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float. Handles common formats."""
    if value is None or pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators for parsing value
        
        # Handle multipliers
        multiplier = 1
        if v.lower().endswith('k'):
            multiplier = 1000
            v = v[:-1]
        elif v.lower().endswith('m'):
            multiplier = 1000000
            v = v[:-1]
            
        if '%' in v:
            return (float(v.replace('%', '')) / 100) * multiplier
            
        # Range handling (e.g., "50-60") - return mean for coercion
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2 * multiplier
            except:
                pass
        try:
            return float(v) * multiplier
        except ValueError:
            return 0.0
    return float(value)

def check_range(value, rule_value):
    """Check if a numeric value fits within a rule's range string (e.g. '100k-1m', '>5')."""
    if rule_value is None:
        return True
        
    # Parse value if it's a string (though it should be float coming in)
    val = float(value)
    
    s = str(rule_value).strip().lower()
    
    # Handle percentages in rule
    is_pct = '%' in s
    
    # Helper to parse rule bounds
    def parse_bound(b):
        b = b.replace('%', '').replace(',', '')
        mult = 1
        if 'k' in b:
            mult = 1000
            b = b.replace('k', '')
        elif 'm' in b:
            mult = 1000000
            b = b.replace('m', '')
        return float(b) * (0.01 if is_pct else 1) * mult

    if '-' in s:
        try:
            low, high = s.split('-')
            return parse_bound(low) <= val <= parse_bound(high)
        except:
            return False
    elif s.startswith('>'):
        return val > parse_bound(s[1:])
    elif s.startswith('<'):
        return val < parse_bound(s[1:])
    elif s == 'immediate':
        return False # Should be handled by string match, not range
    else:
        # Exact match for numbers represented as strings
        try:
            return val == parse_bound(s)
        except:
            return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List membership)
    # Rule has list of allowed types. If null/empty, all allowed.
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List membership)
    if rule.get('merchant_category_code'):
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Exact match or Range)
    if rule.get('capture_delay'):
        rd = rule['capture_delay']
        td = str(tx_ctx['capture_delay'])
        
        # If rule is categorical (immediate/manual), require exact match
        if rd in ['immediate', 'manual']:
            if rd != td:
                return False
        # If rule is numeric/range (e.g., <3, >5)
        else:
            # If merchant delay is categorical (immediate/manual), it generally doesn't match numeric ranges
            # unless we interpret "immediate" as 0. 
            # Based on manual, these are distinct categories.
            if td in ['immediate', 'manual']:
                return False 
            # Otherwise check numeric range
            if not check_range(float(td), rd):
                return False

    # 5. Monthly Volume (Range check)
    if rule.get('monthly_volume'):
        if not check_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 6. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False
            
    # 7. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 8. ACI (List membership)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # JSON uses 0.0/1.0 or boolean.
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
merchant_path = '/output/chunk4/data/context/merchant_data.json'
fees_path = '/output/chunk4/data/context/fees.json'

df = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Filter for Target Merchant and Time Period (Nov 2023)
target_merchant = 'Martinis_Fine_Steakhouse'
target_year = 2023
nov_start = 305
nov_end = 334

# Filter DataFrame
df_nov = df[
    (df['merchant'] == target_merchant) &
    (df['year'] == target_year) &
    (df['day_of_year'] >= nov_start) &
    (df['day_of_year'] <= nov_end)
].copy()

# 3. Get Merchant Context (Static Data)
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

m_account_type = merchant_info['account_type']
m_mcc = merchant_info['merchant_category_code']
m_capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats (Volume & Fraud)
# "Monthly volumes and rates are computed always in natural months"
monthly_volume = df_nov['eur_amount'].sum()

fraud_txs = df_nov[df_nov['has_fraudulent_dispute'] == True]
monthly_fraud_vol = fraud_txs['eur_amount'].sum()

# Fraud level is ratio of fraud volume to total volume
if monthly_volume > 0:
    monthly_fraud_level = monthly_fraud_vol / monthly_volume
else:
    monthly_fraud_level = 0.0

# 5. Calculate Fees per Transaction
total_fees = 0.0

for _, tx in df_nov.iterrows():
    # Build Transaction Context
    # Determine intracountry: Issuer == Acquirer
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'account_type': m_account_type,
        'merchant_category_code': m_mcc,
        'capture_delay': m_capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level,
        'is_credit': bool(tx['is_credit']),
        'aci': tx['aci'],
        'intracountry': is_intracountry
    }
    
    # Find matching rule
    # Iterate through fees.json and take the FIRST match
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(tx_ctx, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee

# 6. Output Result
print(f"{total_fees:.2f}")

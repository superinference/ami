# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1689
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9413 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

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
        # Range handling (e.g., "50-60") - return mean
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
    """Parses a range string like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if range_str is None:
        return (-float('inf'), float('inf'))
    
    s = str(range_str).strip().lower()
    
    # Handle percentages
    is_percent = '%' in s
    s = s.replace('%', '')
    
    # Handle k/m suffixes
    def parse_val(val_s):
        val_s = val_s.strip()
        mult = 1
        if val_s.endswith('k'):
            mult = 1000
            val_s = val_s[:-1]
        elif val_s.endswith('m'):
            mult = 1000000
            val_s = val_s[:-1]
        try:
            return float(val_s) * mult
        except:
            return 0.0

    if is_percent:
        # If it was a percentage, we want the float value (e.g. 8.3 -> 0.083)
        # But wait, parse_val returns the number. We divide by 100 later or handle here.
        # Let's handle scaling after parsing.
        scale = 0.01
    else:
        scale = 1.0

    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]) * scale, parse_val(parts[1]) * scale)
    elif s.startswith('>'):
        return (parse_val(s[1:]) * scale, float('inf'))
    elif s.startswith('<'):
        return (-float('inf'), parse_val(s[1:]) * scale)
    else:
        # Exact match treated as range [val, val]
        val = parse_val(s) * scale
        return (val, val)

def check_range(value, range_str):
    """Checks if a value falls within a range string."""
    if range_str is None:
        return True
    min_val, max_val = parse_range(range_str)
    # Handle edge cases for inclusive/exclusive if needed, but simple comparison usually suffices
    # For this dataset, ranges are typically inclusive
    return min_val <= value <= max_val

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_context must contain:
    - card_scheme
    - account_type
    - merchant_category_code
    - capture_delay
    - is_credit
    - aci
    - intracountry
    - monthly_volume
    - monthly_fraud_level
    """
    
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    # If rule['account_type'] is empty/null, it applies to all.
    # If not empty, merchant's account_type must be in the list.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Exact match or Range?)
    # The manual says "Possible values are '3-5', '>5', '<3', 'immediate', or 'manual'".
    # The merchant data has specific values like 'manual', 'immediate', '1'.
    # If rule is null, applies to all.
    if rule.get('capture_delay'):
        rule_cd = str(rule['capture_delay'])
        tx_cd = str(tx_context['capture_delay'])
        
        # Direct match check first (e.g. "manual" == "manual")
        if rule_cd == tx_cd:
            pass
        # Range check for numeric delays
        elif any(c.isdigit() for c in rule_cd) and any(c.isdigit() for c in tx_cd):
            # Try to parse tx_cd as number
            try:
                delay_days = float(tx_cd)
                if not check_range(delay_days, rule_cd):
                    return False
            except ValueError:
                # tx_cd might be "immediate" or "manual" but rule is numeric range
                # "immediate" could be 0 days? "manual" is undefined/infinite?
                # Based on data, usually they match strings or are distinct categories.
                # If we can't parse, and strings didn't match above, assume no match.
                return False
        else:
            # Strings didn't match and not both numeric
            return False

    # 5. Is Credit (Boolean match)
    # If rule['is_credit'] is null, applies to both.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 6. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Boolean match)
    # If rule['intracountry'] is null, applies to both.
    # Note: fees.json has 0.0/1.0 or null. 1.0 is True?
    if rule.get('intracountry') is not None:
        # Convert rule value to bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    return True

# ==========================================
# MAIN EXECUTION
# ==========================================

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Define Target Context
target_merchant = "Crossfit_Hanna"
target_year = 2023
target_day_of_year = 100

# 3. Get Merchant Static Data
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 4. Calculate Monthly Stats (Volume & Fraud)
# Day 100 of 2023 is in April.
# Jan: 31, Feb: 28, Mar: 31 = 90 days.
# April is days 91 to 120.
month_start_day = 91
month_end_day = 120

# Filter for the whole month of April 2023 for this merchant
df_month = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= month_start_day) &
    (df_payments['day_of_year'] <= month_end_day)
]

# Calculate Volume (Sum of EUR)
monthly_volume = df_month['eur_amount'].sum()

# Calculate Fraud Level (Fraud Volume / Total Volume)
# Manual: "ratio between monthly total volume and monthly volume notified as fraud"
fraud_volume = df_month[df_month['has_fraudulent_dispute'] == True]['eur_amount'].sum()
if monthly_volume > 0:
    monthly_fraud_level = fraud_volume / monthly_volume
else:
    monthly_fraud_level = 0.0

print(f"Merchant: {target_merchant}")
print(f"Month (Day {month_start_day}-{month_end_day}): Volume = {monthly_volume:.2f}, Fraud Level = {monthly_fraud_level:.4%}")

# 5. Filter Target Transactions (Day 100)
df_target = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] == target_day_of_year)
]

print(f"Transactions on Day {target_day_of_year}: {len(df_target)}")

# 6. Match Fees
applicable_fee_ids = set()

for _, tx in df_target.iterrows():
    # Construct transaction context
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    tx_context = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'merchant_category_code': mcc,
        'capture_delay': capture_delay,
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': is_intracountry,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    # Check against all rules
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            applicable_fee_ids.add(rule['ID'])

# 7. Output Result
sorted_ids = sorted(list(applicable_fee_ids))
print(f"Applicable Fee IDs: {sorted_ids}")

# Format for final answer (comma separated string)
result_str = ", ".join(map(str, sorted_ids))
print(result_str)

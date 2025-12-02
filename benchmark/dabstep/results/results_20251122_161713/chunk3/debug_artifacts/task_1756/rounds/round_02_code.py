# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1756
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9139 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import re

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

def parse_range_check(value, rule_string):
    """
    Checks if a numeric value fits within a rule string (e.g., '>5', '100k-1m', '8.3%').
    Returns True if match, False otherwise.
    """
    if rule_string is None:
        return True
        
    # Handle percentages in rule
    is_percentage = '%' in rule_string
    
    # Clean up rule string for parsing
    clean_rule = rule_string.replace('%', '').replace(',', '').replace('€', '').replace('$', '').strip()
    
    # Handle k/m suffixes for volume
    def parse_val(s):
        s = s.lower()
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

    # Adjust value if comparison is percentage based
    # If rule is "8%", it means 0.08. If value is 0.09 (9%), it should compare 0.09 vs 0.08
    # The coerce_to_float helper handles the value conversion before calling this, 
    # but we need to ensure the rule limits are parsed correctly.
    
    if '-' in clean_rule:
        # Range: "100k-1m" or "0-1"
        parts = clean_rule.split('-')
        if len(parts) == 2:
            low = parse_val(parts[0])
            high = parse_val(parts[1])
            if is_percentage:
                low /= 100
                high /= 100
            return low <= value <= high
            
    elif clean_rule.startswith('>'):
        limit = parse_val(clean_rule[1:])
        if is_percentage:
            limit /= 100
        return value > limit
        
    elif clean_rule.startswith('<'):
        limit = parse_val(clean_rule[1:])
        if is_percentage:
            limit /= 100
        return value < limit
        
    elif clean_rule.startswith('>='):
        limit = parse_val(clean_rule[2:])
        if is_percentage:
            limit /= 100
        return value >= limit
        
    elif clean_rule.startswith('<='):
        limit = parse_val(clean_rule[2:])
        if is_percentage:
            limit /= 100
        return value <= limit
        
    return False

def check_capture_delay(merchant_delay, rule_delay):
    """
    Matches merchant capture delay against rule.
    Merchant delay: "1", "manual", "immediate"
    Rule delay: "3-5", ">5", "<3", "immediate", "manual", None
    """
    if rule_delay is None:
        return True
    
    # Exact string match (e.g., "manual", "immediate")
    if str(merchant_delay).lower() == str(rule_delay).lower():
        return True
        
    # If merchant delay is numeric (days), parse it
    try:
        delay_days = float(merchant_delay)
    except ValueError:
        # If merchant delay is "immediate", treat as 0 days
        if str(merchant_delay).lower() == 'immediate':
            delay_days = 0
        else:
            # If merchant is "manual" but rule is numeric range, usually doesn't match
            return False

    # Parse rule logic
    return parse_range_check(delay_days, rule_delay)

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_context must contain:
      - card_scheme (str)
      - account_type (str)
      - mcc (int)
      - is_credit (bool)
      - aci (str)
      - intracountry (bool)
      - monthly_volume (float)
      - monthly_fraud_rate (float)
      - capture_delay (str)
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    # Rule has list of allowed types. If empty/null, allows all.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Boolean match)
    # If rule is null, applies to both.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean match)
    # If rule is null, applies to both.
    if rule.get('intracountry') is not None:
        # JSON uses 0.0/1.0 for boolean often, or true/false
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False
            
    # 7. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_context['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False
            
    # 9. Capture Delay (Complex match)
    if rule.get('capture_delay'):
        if not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
            return False
            
    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# File paths
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Define Target
target_merchant = 'Belles_cookbook_store'
target_year = 2023
start_day = 152  # June 1st
end_day = 181    # June 30th

# 3. Filter Transactions for Merchant + Month
# We need the whole month to calculate monthly stats correctly
month_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= start_day) &
    (df_payments['day_of_year'] <= end_day)
].copy()

# 4. Get Merchant Attributes
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

mcc = merchant_info['merchant_category_code']
account_type = merchant_info['account_type']
capture_delay = merchant_info['capture_delay']

# 5. Calculate Monthly Stats
# Volume in Euros
monthly_volume = month_txs['eur_amount'].sum()

# Fraud Rate (Count of fraud / Total count)
# Note: 'has_fraudulent_dispute' is boolean
fraud_count = month_txs['has_fraudulent_dispute'].sum()
total_count = len(month_txs)
monthly_fraud_rate = (fraud_count / total_count) if total_count > 0 else 0.0

# 6. Identify Applicable Fees
applicable_fee_ids = set()

# Iterate through every transaction in the month
for _, tx in month_txs.iterrows():
    # Determine intracountry status
    # True if issuing_country == acquirer_country
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    # Build context for this transaction
    tx_context = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'mcc': mcc,
        'is_credit': bool(tx['is_credit']),
        'aci': tx['aci'],
        'intracountry': is_intracountry,
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate,
        'capture_delay': capture_delay
    }
    
    # Check against all fee rules
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            applicable_fee_ids.add(rule['ID'])

# 7. Output Results
# Convert to sorted list
sorted_ids = sorted(list(applicable_fee_ids))

# Print just the list of IDs as requested by the question style
print(", ".join(map(str, sorted_ids)))

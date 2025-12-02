# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1801
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8854 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, k, m, commas to float."""
    if value is None or value == '':
        return None
    if isinstance(value, (int, float)):
        return float(value)
    
    s = str(value).strip().lower().replace(',', '').replace('€', '').replace('$', '')
    
    # Handle percentages
    if '%' in s:
        return float(s.replace('%', '')) / 100.0
    
    # Handle k/m suffixes
    if s.endswith('k'):
        return float(s[:-1]) * 1000
    if s.endswith('m'):
        return float(s[:-1]) * 1000000
        
    # Handle comparison operators (just strip for raw value, logic handled elsewhere if needed)
    s = s.lstrip('><≤≥=')
    
    try:
        return float(s)
    except ValueError:
        return None

def parse_range(range_str):
    """
    Parses a range string like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max).
    Returns (min_val, max_val). None indicates no limit.
    """
    if not range_str:
        return (None, None)
    
    s = str(range_str).strip().lower()
    
    # Handle Greater Than
    if s.startswith('>'):
        val = coerce_to_float(s[1:])
        return (val, float('inf'))
    
    # Handle Less Than
    if s.startswith('<'):
        val = coerce_to_float(s[1:])
        return (float('-inf'), val)
        
    # Handle Range (hyphen)
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            return (coerce_to_float(parts[0]), coerce_to_float(parts[1]))
            
    # Handle Exact Match (treated as range [val, val])
    val = coerce_to_float(s)
    if val is not None:
        return (val, val)
        
    return (None, None)

def is_in_range(value, range_str):
    """Checks if a numeric value falls within a parsed range string."""
    if range_str is None:
        return True
    
    min_val, max_val = parse_range(range_str)
    
    if min_val is not None and value <= min_val and max_val == float('inf'):
        # Strict inequality for '>' usually implies > val, but context implies inclusive often.
        # Let's assume standard mathematical interpretation of >.
        # However, for '100k-1m', it's inclusive.
        # For '>5', let's assume strictly greater.
        if str(range_str).startswith('>'):
            return value > min_val
        return value >= min_val
        
    if max_val is not None and value >= max_val and min_val == float('-inf'):
        if str(range_str).startswith('<'):
            return value < max_val
        return value <= max_val
        
    if min_val is not None and max_val is not None:
        return min_val <= value <= max_val
        
    return True

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_context must contain:
      - card_scheme (str)
      - account_type (str)
      - capture_delay (str)
      - merchant_category_code (int)
      - is_credit (bool)
      - aci (str)
      - intracountry (bool)
      - monthly_volume (float)
      - monthly_fraud_rate (float)
    """
    
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    # If rule['account_type'] is empty/null, it applies to all.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Capture Delay (Range/Exact match)
    # Note: capture_delay in merchant_data is string ('manual', 'immediate', '1').
    # Rule might be '>5', 'manual', etc.
    if rule.get('capture_delay'):
        rule_delay = str(rule['capture_delay'])
        tx_delay = str(tx_context['capture_delay'])
        
        # Handle numeric comparison if both look numeric
        if rule_delay[0] in '<>' and tx_delay.replace('.', '', 1).isdigit():
            # Convert tx_delay to float for comparison
            if not is_in_range(float(tx_delay), rule_delay):
                return False
        elif rule_delay != tx_delay:
            # String exact match (e.g., 'manual' == 'manual')
            return False

    # 4. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
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
        # Note: rule['intracountry'] might be 0.0/1.0 or False/True
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False
            
    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not is_in_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not is_in_range(tx_context['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False
            
    return True

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter for Rafa_AI in March 2023
# March 2023 (non-leap) is Day 60 to 90 inclusive.
target_merchant = 'Rafa_AI'
df_march = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == 2023) &
    (df_payments['day_of_year'] >= 60) &
    (df_payments['day_of_year'] <= 90)
].copy()

# 3. Get Merchant Static Data
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

# 4. Calculate Monthly Stats (Volume & Fraud Rate)
# Volume is sum of eur_amount
monthly_volume = df_march['eur_amount'].sum()

# Fraud Volume is sum of eur_amount where has_fraudulent_dispute is True
fraud_volume = df_march[df_march['has_fraudulent_dispute'] == True]['eur_amount'].sum()

# Fraud Rate = Fraud Volume / Total Volume
# Handle division by zero
if monthly_volume > 0:
    monthly_fraud_rate = fraud_volume / monthly_volume
else:
    monthly_fraud_rate = 0.0

print(f"Merchant: {target_merchant}")
print(f"March 2023 Volume: €{monthly_volume:,.2f}")
print(f"March 2023 Fraud Volume: €{fraud_volume:,.2f}")
print(f"March 2023 Fraud Rate: {monthly_fraud_rate:.4%}")

# 5. Identify Applicable Fee IDs
# We need to check every transaction (or unique transaction profile) against every fee rule.
# To optimize, we group transactions by the fields that vary in fee rules:
# card_scheme, is_credit, aci, issuing_country, acquirer_country

# Create 'intracountry' column for grouping
df_march['intracountry'] = df_march['issuing_country'] == df_march['acquirer_country']

# Get unique profiles
unique_profiles = df_march[[
    'card_scheme', 'is_credit', 'aci', 'intracountry'
]].drop_duplicates().to_dict('records')

applicable_fee_ids = set()

for profile in unique_profiles:
    # Construct full context for matching
    context = {
        # Transaction specific
        'card_scheme': profile['card_scheme'],
        'is_credit': profile['is_credit'],
        'aci': profile['aci'],
        'intracountry': profile['intracountry'],
        
        # Merchant specific (Static)
        'account_type': merchant_info['account_type'],
        'merchant_category_code': merchant_info['merchant_category_code'],
        'capture_delay': merchant_info['capture_delay'],
        
        # Monthly Aggregates
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }
    
    # Check against all fee rules
    for rule in fees_data:
        if match_fee_rule(context, rule):
            applicable_fee_ids.add(rule['ID'])

# 6. Output Result
sorted_ids = sorted(list(applicable_fee_ids))
print("\nApplicable Fee IDs:")
print(", ".join(map(str, sorted_ids)))

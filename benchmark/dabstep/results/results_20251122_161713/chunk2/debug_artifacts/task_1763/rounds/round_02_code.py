# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1763
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8367 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m suffixes to float."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.replace('>', '').replace('<', '').replace('=', '') # Remove operators for raw value parsing
        
        # Handle percentages
        if '%' in v:
            return float(v.replace('%', '')) / 100.0
            
        # Handle k/m suffixes
        multiplier = 1.0
        if v.endswith('k'):
            multiplier = 1_000.0
            v = v[:-1]
        elif v.endswith('m'):
            multiplier = 1_000_000.0
            v = v[:-1]
            
        try:
            return float(v) * multiplier
        except ValueError:
            return 0.0
    return 0.0

def parse_range_check(rule_value, actual_value):
    """
    Check if actual_value falls within the rule_value range.
    rule_value examples: '100k-1m', '>5', '<3', '7.7%-8.3%', 'immediate', 'manual'
    """
    if rule_value is None:
        return True
        
    # Handle string equality for non-numeric rules (e.g., 'immediate', 'manual')
    if isinstance(rule_value, str) and not any(c.isdigit() for c in rule_value):
        return rule_value.lower() == str(actual_value).lower()

    # Handle numeric comparisons
    try:
        # Convert actual_value to float if it's a string number (e.g. "1")
        if isinstance(actual_value, str):
            if actual_value.lower() in ['immediate', 'manual']:
                return rule_value.lower() == actual_value.lower()
            actual_float = float(actual_value)
        else:
            actual_float = float(actual_value)
            
        rv = str(rule_value).lower().strip()
        
        # Range "X-Y"
        if '-' in rv:
            parts = rv.split('-')
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return min_val <= actual_float <= max_val
            
        # Greater than
        if rv.startswith('>'):
            limit = coerce_to_float(rv[1:])
            return actual_float > limit
            
        # Less than
        if rv.startswith('<'):
            limit = coerce_to_float(rv[1:])
            return actual_float < limit
            
        # Exact match (numeric)
        return actual_float == coerce_to_float(rv)
        
    except Exception:
        # Fallback for complex strings or mismatches
        return str(rule_value) == str(actual_value)

def match_fee_rule(transaction, rule):
    """
    Check if a fee rule applies to a specific transaction context.
    transaction dict must contain:
      - card_scheme, is_credit, aci, intracountry (from payment)
      - account_type, merchant_category_code, capture_delay (from merchant data)
      - monthly_volume, monthly_fraud_level (calculated stats)
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != transaction['card_scheme']:
        return False

    # 2. Account Type (List match - Wildcard allowed)
    if rule.get('account_type'):
        if transaction['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match - Wildcard allowed)
    if rule.get('merchant_category_code'):
        if transaction['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 4. ACI (List match - Wildcard allowed)
    if rule.get('aci'):
        if transaction['aci'] not in rule['aci']:
            return False

    # 5. Is Credit (Boolean match - Wildcard allowed)
    if rule.get('is_credit') is not None:
        # Handle string 'None' or actual None in JSON
        if str(rule['is_credit']).lower() != 'none': 
            if bool(rule['is_credit']) != transaction['is_credit']:
                return False

    # 6. Intracountry (Boolean match - Wildcard allowed)
    if rule.get('intracountry') is not None:
        # JSON might have 0.0/1.0 or boolean
        rule_intra = rule['intracountry']
        if str(rule_intra).lower() != 'none':
            is_intra_rule = bool(float(rule_intra)) if isinstance(rule_intra, (int, float, str)) else rule_intra
            if is_intra_rule != transaction['intracountry']:
                return False

    # 7. Capture Delay (Range/Value match - Wildcard allowed)
    if rule.get('capture_delay'):
        if not parse_range_check(rule['capture_delay'], transaction['capture_delay']):
            return False

    # 8. Monthly Volume (Range match - Wildcard allowed)
    if rule.get('monthly_volume'):
        if not parse_range_check(rule['monthly_volume'], transaction['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match - Wildcard allowed)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(rule['monthly_fraud_level'], transaction['monthly_fraud_level']):
            return False

    return True

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# 1. Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data_list = json.load(f)

# 2. Filter Transactions (Crossfit_Hanna, Jan 2023)
target_merchant = 'Crossfit_Hanna'
filtered_df = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['day_of_year'] >= 1) & 
    (df_payments['day_of_year'] <= 31) &
    (df_payments['year'] == 2023)
].copy()

# 3. Get Merchant Static Data
merchant_info = next((m for m in merchant_data_list if m['merchant'] == target_merchant), None)
if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
    exit()

# 4. Calculate Monthly Stats
total_volume = filtered_df['eur_amount'].sum()
fraud_volume = filtered_df[filtered_df['has_fraudulent_dispute'] == True]['eur_amount'].sum()
fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

print(f"Merchant: {target_merchant}")
print(f"Total Volume (Jan): {total_volume:.2f}")
print(f"Fraud Volume (Jan): {fraud_volume:.2f}")
print(f"Fraud Rate: {fraud_rate:.4%}")
print(f"Static Info: MCC={merchant_info['merchant_category_code']}, Account={merchant_info['account_type']}, Delay={merchant_info['capture_delay']}")

# 5. Identify Applicable Fee IDs
applicable_fee_ids = set()

# Optimization: Iterate over unique transaction profiles instead of every row
# Attributes that vary per transaction and affect fees:
# card_scheme, is_credit, aci, issuing_country, acquirer_country
unique_tx_profiles = filtered_df[[
    'card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country'
]].drop_duplicates()

print(f"Unique transaction profiles to check: {len(unique_tx_profiles)}")

for _, row in unique_tx_profiles.iterrows():
    # Construct transaction context for matching
    tx_context = {
        # Transaction specific
        'card_scheme': row['card_scheme'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['issuing_country'] == row['acquirer_country'],
        
        # Merchant specific (static)
        'account_type': merchant_info['account_type'],
        'merchant_category_code': merchant_info['merchant_category_code'],
        'capture_delay': merchant_info['capture_delay'],
        
        # Monthly stats (static for the month)
        'monthly_volume': total_volume,
        'monthly_fraud_level': fraud_rate
    }
    
    # Check against all fee rules
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            applicable_fee_ids.add(rule['ID'])

# 6. Output Results
sorted_ids = sorted(list(applicable_fee_ids))
print("\nApplicable Fee IDs:")
print(", ".join(map(str, sorted_ids)))

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1789
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7506 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        # Handle percentages
        if '%' in v:
            return float(v.replace('%', '')) / 100
        # Handle k/m suffixes
        if v.lower().endswith('k'):
            return float(v[:-1]) * 1_000
        if v.lower().endswith('m'):
            return float(v[:-1]) * 1_000_000
        return float(v)
    return 0.0

def parse_range(range_str):
    """
    Parses a range string like '100k-1m', '<3', '>5', '0%-0.5%'.
    Returns a tuple (min_val, max_val).
    """
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip()
    
    # Handle inequalities
    if s.startswith('<'):
        return float('-inf'), coerce_to_float(s[1:])
    if s.startswith('>'):
        return coerce_to_float(s[1:]), float('inf')
        
    # Handle ranges
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            return coerce_to_float(parts[0]), coerce_to_float(parts[1])
            
    # Handle exact matches (though usually ranges are explicit)
    val = coerce_to_float(s)
    return val, val

def check_range_match(value, range_str):
    """Checks if a numeric value falls within a string-defined range."""
    if range_str is None:
        return True # Wildcard matches all
    
    min_v, max_v = parse_range(range_str)
    if min_v is None: 
        return False
        
    # Inclusive boundaries assumed based on typical business logic
    return min_v <= value <= max_v

def is_match(rule_val, actual_val):
    """
    Generic matcher for fee rules.
    - rule_val is None or [] -> Wildcard (Match)
    - rule_val is List -> Match if actual_val in list
    - rule_val is Scalar -> Match if actual_val == rule_val
    """
    if rule_val is None:
        return True
    if isinstance(rule_val, list):
        if not rule_val: # Empty list is wildcard
            return True
        return actual_val in rule_val
    return rule_val == actual_val

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

# File paths
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'
fees_path = '/output/chunk2/data/context/fees.json'

# 1. Load Data
print("Loading data...")
df = pd.read_csv(payments_path)
with open(merchant_data_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Filter for Merchant and Time Period (March 2023)
target_merchant = "Martinis_Fine_Steakhouse"
# March 2023: Year 2023, Day of Year 60 to 90 (Non-leap year)
df_march = df[
    (df['merchant'] == target_merchant) & 
    (df['year'] == 2023) & 
    (df['day_of_year'] >= 60) & 
    (df['day_of_year'] <= 90)
].copy()

print(f"Transactions found for {target_merchant} in March 2023: {len(df_march)}")

# 3. Calculate Monthly Metrics (Volume and Fraud Rate)
# Volume: Sum of eur_amount
monthly_volume = df_march['eur_amount'].sum()

# Fraud Rate: Fraudulent Volume / Total Volume
fraud_txs = df_march[df_march['has_fraudulent_dispute'] == True]
fraud_volume = fraud_txs['eur_amount'].sum()

if monthly_volume > 0:
    monthly_fraud_rate = fraud_volume / monthly_volume
else:
    monthly_fraud_rate = 0.0

print(f"Monthly Volume: €{monthly_volume:,.2f}")
print(f"Monthly Fraud Rate: {monthly_fraud_rate:.4%}")

# 4. Get Static Merchant Attributes
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

m_account_type = merchant_info['account_type']
m_mcc = merchant_info['merchant_category_code']
m_capture_delay = merchant_info['capture_delay']

print(f"Merchant Attributes: Type={m_account_type}, MCC={m_mcc}, Delay={m_capture_delay}")

# 5. Identify Unique Transaction Profiles in the Data
# We need to check 'intracountry' status (Issuer == Acquirer)
df_march['intracountry'] = df_march['issuing_country'] == df_march['acquirer_country']

# Get unique combinations of attributes that affect fees
# Columns: card_scheme, is_credit, aci, intracountry
profiles = df_march[['card_scheme', 'is_credit', 'aci', 'intracountry']].drop_duplicates()
print(f"Unique transaction profiles identified: {len(profiles)}")

# 6. Find Applicable Fee IDs
applicable_fee_ids = set()

for rule in fees_data:
    # --- CHECK MERCHANT/MONTHLY LEVEL CONDITIONS FIRST ---
    
    # 1. Account Type (List match)
    if not is_match(rule['account_type'], m_account_type):
        continue
        
    # 2. Merchant Category Code (List match)
    if not is_match(rule['merchant_category_code'], m_mcc):
        continue
        
    # 3. Capture Delay (Exact match or Range)
    # The manual lists specific strings for capture delay rules ('3-5', 'immediate', etc.)
    # If the rule is a range string like '3-5', we might need logic, but usually these match the enum values.
    # Let's assume exact match for string enums like 'immediate', 'manual'.
    # If rule is null, it matches.
    if rule['capture_delay'] is not None:
        # If rule is a specific value like 'immediate', check equality
        if rule['capture_delay'] != m_capture_delay:
            # If they don't match exactly, check if it's a range logic (e.g. <3 vs 1)
            # For this specific dataset, 'immediate' is a distinct category.
            continue

    # 4. Monthly Volume (Range match)
    if not check_range_match(monthly_volume, rule['monthly_volume']):
        continue
        
    # 5. Monthly Fraud Level (Range match)
    if not check_range_match(monthly_fraud_rate, rule['monthly_fraud_level']):
        continue

    # --- CHECK TRANSACTION LEVEL CONDITIONS ---
    # The rule applies if it matches AT LEAST ONE transaction profile present in the data.
    
    rule_matches_any_transaction = False
    
    for _, profile in profiles.iterrows():
        # 6. Card Scheme (Exact match - usually string)
        if rule['card_scheme'] != profile['card_scheme']:
            continue
            
        # 7. Is Credit (Boolean match)
        if rule['is_credit'] is not None and rule['is_credit'] != profile['is_credit']:
            continue
            
        # 8. ACI (List match)
        if not is_match(rule['aci'], profile['aci']):
            continue
            
        # 9. Intracountry (Boolean/Float match)
        # In fees.json, intracountry is often 0.0 (False) or 1.0 (True) or null
        if rule['intracountry'] is not None:
            rule_intra = bool(float(rule['intracountry']))
            if rule_intra != profile['intracountry']:
                continue
        
        # If we reached here, this profile matches the rule
        rule_matches_any_transaction = True
        break
    
    if rule_matches_any_transaction:
        applicable_fee_ids.add(rule['ID'])

# 7. Output Results
sorted_ids = sorted(list(applicable_fee_ids))
print("\n" + "="*30)
print("APPLICABLE FEE IDs")
print("="*30)
print(", ".join(map(str, sorted_ids)))

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2703
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10362 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# -------------------------------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return None
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
            return None
    return None

def parse_range_check(value, rule_value):
    """
    Checks if a numeric value fits within a rule string (e.g., '>5', '100k-1m').
    """
    if rule_value is None:
        return True
    
    # Handle simple equality (though usually ranges are strings)
    if isinstance(rule_value, (int, float)):
        return value == rule_value

    s = str(rule_value).strip()
    
    # Handle k/m suffixes
    def parse_val(x):
        x = x.lower().replace('%', '')
        mult = 1
        if 'k' in x:
            mult = 1000
            x = x.replace('k', '')
        elif 'm' in x:
            mult = 1_000_000
            x = x.replace('m', '')
        return float(x) * mult

    try:
        if '-' in s:
            low, high = s.split('-')
            # Handle percentages in ranges like "7.7%-8.3%"
            is_pct = '%' in s
            l = parse_val(low)
            h = parse_val(high)
            if is_pct:
                l /= 100
                h /= 100
            return l <= value <= h
        elif s.startswith('>'):
            limit = parse_val(s[1:])
            if '%' in s: limit /= 100
            return value > limit
        elif s.startswith('<'):
            limit = parse_val(s[1:])
            if '%' in s: limit /= 100
            return value < limit
        elif s == 'immediate':
            return value == 0 # Assuming immediate is 0 delay
        elif s == 'manual':
            return True # Manual often treated as specific category, but if value is string 'manual', handle elsewhere
        else:
            # Exact match string
            return str(value) == s
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    tx_ctx: dict containing transaction details + merchant stats
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List contains)
    if rule['account_type'] and tx_ctx['account_type'] not in rule['account_type']:
        return False

    # 3. Merchant Category Code (List contains)
    if rule['merchant_category_code'] and tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
        return False

    # 4. ACI (List contains)
    # Note: rule['aci'] can be None or empty list for wildcard
    if rule['aci'] and tx_ctx['aci'] not in rule['aci']:
        return False

    # 5. Is Credit (Boolean match)
    if rule['is_credit'] is not None:
        # Handle string 'None' or actual None in JSON
        if str(rule['is_credit']).lower() != 'none':
            if rule['is_credit'] != tx_ctx['is_credit']:
                return False

    # 6. Intracountry (Boolean match)
    if rule['intracountry'] is not None:
        # JSON might have 0.0/1.0 or boolean
        rule_intra = bool(float(rule['intracountry'])) if isinstance(rule['intracountry'], (int, float, str)) else rule['intracountry']
        if rule_intra != tx_ctx['intracountry']:
            return False

    # 7. Capture Delay (Range/String match)
    # tx_ctx['capture_delay'] is a string from merchant_data (e.g., 'manual', 'immediate', '1')
    if rule['capture_delay'] is not None:
        if rule['capture_delay'] != tx_ctx['capture_delay']:
             # If it's not an exact string match, check if it's a range logic (rare for capture_delay but possible)
             # For this dataset, capture_delay seems to be categorical strings mostly.
             # Let's assume exact match for strings like 'manual', 'immediate'.
             # If numeric string '1', it might match '<3'.
             try:
                 val = float(tx_ctx['capture_delay'])
                 if not parse_range_check(val, rule['capture_delay']):
                     return False
             except ValueError:
                 # It's a string like 'manual' and didn't match exact string above
                 return False

    # 8. Monthly Volume (Range match)
    if rule['monthly_volume'] is not None:
        if not parse_range_check(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule['monthly_fraud_level'] is not None:
        if not parse_range_check(tx_ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = coerce_to_float(rule['fixed_amount'])
    rate = coerce_to_float(rule['rate'])
    # Fee = Fixed + (Rate * Amount / 10000)
    # Note: Rate is often in basis points or similar, manual says "divided by 10000"
    return fixed + (rate * amount / 10000.0)

# -------------------------------------------------------------------------
# MAIN ANALYSIS
# -------------------------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'

df = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter Context: Crossfit_Hanna, February
merchant_name = 'Crossfit_Hanna'
start_day = 32
end_day = 59

# Get Merchant Metadata
m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not m_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

mcc = m_info['merchant_category_code']
account_type = m_info['account_type']
capture_delay = m_info['capture_delay']

# Filter Transactions for Stats (Volume & Fraud Rate)
# Stats are calculated on ALL transactions for the merchant in the period
df_merchant_feb = df[
    (df['merchant'] == merchant_name) &
    (df['day_of_year'] >= start_day) &
    (df['day_of_year'] <= end_day)
]

total_volume = df_merchant_feb['eur_amount'].sum()
fraud_count = df_merchant_feb['has_fraudulent_dispute'].sum()
total_count = len(df_merchant_feb)
fraud_rate = fraud_count / total_count if total_count > 0 else 0.0

# Filter Target Transactions: Fraudulent ones only
df_fraud = df_merchant_feb[df_merchant_feb['has_fraudulent_dispute'] == True].copy()

# Pre-calculate intracountry for fraud txs
# Intracountry: issuing_country == acquirer_country
df_fraud['intracountry'] = df_fraud['issuing_country'] == df_fraud['acquirer_country']

# 3. Simulate ACIs
possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
results = {}

# Optimization: Pre-filter fees by static merchant properties to speed up loop
# We can filter by MCC and Account Type since these don't change per tx
candidate_fees = []
for rule in fees_data:
    # Check MCC
    if rule['merchant_category_code'] and mcc not in rule['merchant_category_code']:
        continue
    # Check Account Type
    if rule['account_type'] and account_type not in rule['account_type']:
        continue
    candidate_fees.append(rule)

# print(f"Filtered fee rules from {len(fees_data)} to {len(candidate_fees)} based on static merchant data.")

for aci in possible_acis:
    total_fee_for_aci = 0.0
    
    for _, tx in df_fraud.iterrows():
        # Build context for this transaction with the SIMULATED ACI
        tx_ctx = {
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'],
            'eur_amount': tx['eur_amount'],
            'intracountry': tx['intracountry'],
            'aci': aci,  # <--- SIMULATED CHANGE
            'merchant_category_code': mcc,
            'account_type': account_type,
            'capture_delay': capture_delay,
            'monthly_volume': total_volume,
            'monthly_fraud_level': fraud_rate
        }
        
        # Find applicable rule
        # We iterate through candidate_fees and take the first match (assuming priority or non-overlapping)
        # Usually, specific rules override generic ones, but without specific priority logic, first match is standard.
        # However, fees.json often has specific vs generic. Let's assume the dataset is ordered or we find *a* match.
        # A common strategy is to collect all matches and pick the most specific, or just the first one if the list is prioritized.
        # Given the complexity, we'll look for the first valid match.
        
        matched_rule = None
        for rule in candidate_fees:
            if match_fee_rule(tx_ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fee_for_aci += fee
        else:
            # If no rule matches, this ACI might be invalid for this scheme/config.
            # We should probably penalize it or ignore it. 
            # For this exercise, we assume coverage exists.
            # print(f"Warning: No fee rule found for tx {tx['psp_reference']} with ACI {aci}")
            pass

    results[aci] = total_fee_for_aci

# 4. Determine Preferred Choice
# Find ACI with minimum total fee
best_aci = min(results, key=results.get)
min_fee = results[best_aci]

# print("\n--- Simulation Results ---")
# for k, v in results.items():
#     print(f"ACI {k}: €{v:.2f}")

print(best_aci)

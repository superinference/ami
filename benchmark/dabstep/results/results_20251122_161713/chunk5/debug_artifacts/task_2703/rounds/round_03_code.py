# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2703
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 9031 characters (FULL CODE)
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
    Also handles string equality for categorical fields.
    """
    if rule_value is None:
        return True
    
    # Handle explicit null string in JSON if any
    if str(rule_value).lower() == 'none':
        return True

    # Exact match for numbers
    if isinstance(rule_value, (int, float)):
        return value == rule_value

    s = str(rule_value).strip()
    
    # Handle exact string matches for categorical fields like 'manual'
    if isinstance(value, str):
        return s.lower() == value.lower()

    # Helper to parse k/m suffixes
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
        # Ensure value is numeric for range checks
        val_num = float(value)
        
        if '-' in s:
            low, high = s.split('-')
            is_pct = '%' in s
            l = parse_val(low)
            h = parse_val(high)
            if is_pct:
                l /= 100
                h /= 100
            return l <= val_num <= h
        elif s.startswith('>'):
            limit = parse_val(s[1:])
            if '%' in s: limit /= 100
            return val_num > limit
        elif s.startswith('<'):
            limit = parse_val(s[1:])
            if '%' in s: limit /= 100
            return val_num < limit
        else:
            # Try numeric equality
            return val_num == parse_val(s)
    except:
        # Fallback for non-numeric value matching string rule
        return str(value).lower() == s.lower()

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

    # 4. ACI (List contains) - The simulated ACI is in tx_ctx['aci']
    if rule['aci'] and tx_ctx['aci'] not in rule['aci']:
        return False

    # 5. Is Credit (Boolean match)
    if rule['is_credit'] is not None:
        if str(rule['is_credit']).lower() != 'none':
            if rule['is_credit'] != tx_ctx['is_credit']:
                return False

    # 6. Intracountry (Boolean match)
    if rule['intracountry'] is not None:
        rule_intra = rule['intracountry']
        # Handle string '0.0'/'1.0' or bool
        if isinstance(rule_intra, str):
            if rule_intra.lower() == 'true': rule_intra = True
            elif rule_intra.lower() == 'false': rule_intra = False
            else:
                try:
                    rule_intra = bool(float(rule_intra))
                except:
                    pass
        elif isinstance(rule_intra, (int, float)):
            rule_intra = bool(rule_intra)
            
        if rule_intra != tx_ctx['intracountry']:
            return False

    # 7. Capture Delay (Range/String match)
    if rule['capture_delay'] is not None:
        if not parse_range_check(tx_ctx['capture_delay'], rule['capture_delay']):
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
candidate_fees = []
for rule in fees_data:
    # Check MCC
    if rule['merchant_category_code'] and mcc not in rule['merchant_category_code']:
        continue
    # Check Account Type
    if rule['account_type'] and account_type not in rule['account_type']:
        continue
    candidate_fees.append(rule)

for aci in possible_acis:
    total_fee_for_aci = 0.0
    valid_aci = True
    
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
        
        matched_rule = None
        # Find the first applicable rule
        for rule in candidate_fees:
            if match_fee_rule(tx_ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fee_for_aci += fee
        else:
            # If no rule matches, this ACI is invalid for this configuration
            valid_aci = False
            break

    if valid_aci:
        results[aci] = total_fee_for_aci
    else:
        results[aci] = float('inf')

# 4. Determine Preferred Choice
# Find ACI with minimum total fee
best_aci = min(results, key=results.get)

print(best_aci)

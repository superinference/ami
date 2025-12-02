# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1784
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7792 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if '-' in v: # Handle ranges like "50-60" by taking average, though usually handled by range parsers
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

def parse_range_check(value, range_str, is_percentage=False):
    """
    Checks if a numeric value falls within a string range (e.g., '100k-1m', '>5%', '<3').
    """
    if pd.isna(range_str) or range_str is None:
        return True # Null rule matches everything
    
    if not isinstance(range_str, str):
        return True

    s = range_str.lower().replace(',', '').replace(' ', '')
    
    # Handle units
    multiplier = 1.0
    if 'k' in s:
        multiplier = 1_000.0
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1_000_000.0
        s = s.replace('m', '')
    
    # Handle percentage in range string
    if '%' in s:
        multiplier = 0.01
        s = s.replace('%', '')
        
    try:
        if '-' in s:
            parts = s.split('-')
            min_val = float(parts[0]) * multiplier
            max_val = float(parts[1]) * multiplier
            return min_val <= value <= max_val
        elif '>' in s:
            limit = float(s.replace('>', '').replace('=', '')) * multiplier
            return value > limit # Strict inequality usually, but context dependent
        elif '<' in s:
            limit = float(s.replace('<', '').replace('=', '')) * multiplier
            return value < limit
        else:
            # Exact match attempt
            return value == float(s) * multiplier
    except:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_ctx: dict containing transaction and merchant details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List match)
    # Rule field is a list. If not null/empty, merchant's type must be in it.
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match)
    # If rule is null, applies to both. If set, must match.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean/Float match)
    # 0.0/False = International, 1.0/True = Domestic
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['is_intracountry']:
            return False

    # 7. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level'], is_percentage=True):
            return False

    return True

# ==========================================
# MAIN EXECUTION
# ==========================================

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
merchant_path = '/output/chunk4/data/context/merchant_data.json'
fees_path = '/output/chunk4/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Define Target
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023
# October 2023: Days 274 to 304 (Non-leap year)
start_day = 274
end_day = 304

# 3. Get Merchant Static Data
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

merchant_mcc = merchant_info['merchant_category_code']
merchant_account_type = merchant_info['account_type']

# 4. Calculate Monthly Stats (Volume & Fraud)
# Filter for the WHOLE month to get accurate stats
month_txs = df_payments[
    (df_payments['merchant'] == target_merchant) &
    (df_payments['year'] == target_year) &
    (df_payments['day_of_year'] >= start_day) &
    (df_payments['day_of_year'] <= end_day)
].copy()

if len(month_txs) == 0:
    print("No transactions found for this merchant in October 2023.")
else:
    # Calculate Volume (Sum of eur_amount)
    monthly_volume = month_txs['eur_amount'].sum()
    
    # Calculate Fraud Rate (Fraud Volume / Total Volume)
    # Manual: "Fraud is defined as the ratio of fraudulent volume over total volume."
    fraud_volume = month_txs[month_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

    print(f"Merchant: {target_merchant}")
    print(f"Oct 2023 Volume: €{monthly_volume:,.2f}")
    print(f"Oct 2023 Fraud Rate: {monthly_fraud_rate:.4%}")
    print(f"MCC: {merchant_mcc}, Account Type: {merchant_account_type}")

    # 5. Find Applicable Fees
    applicable_fee_ids = set()

    # We iterate through transactions to check transaction-specific rules (scheme, aci, etc.)
    # Optimization: Iterate over unique transaction profiles to save time
    # Profiles defined by: card_scheme, aci, is_credit, issuing_country, acquirer_country
    
    # Add derived column for intracountry
    month_txs['is_intracountry'] = month_txs['issuing_country'] == month_txs['acquirer_country']
    
    # Define columns that affect fee matching
    match_cols = ['card_scheme', 'aci', 'is_credit', 'is_intracountry']
    unique_profiles = month_txs[match_cols].drop_duplicates()

    print(f"Checking {len(unique_profiles)} unique transaction profiles against {len(fees_data)} fee rules...")

    for _, profile in unique_profiles.iterrows():
        # Build context for this profile
        tx_ctx = {
            'card_scheme': profile['card_scheme'],
            'aci': profile['aci'],
            'is_credit': profile['is_credit'],
            'is_intracountry': profile['is_intracountry'],
            'mcc': merchant_mcc,
            'account_type': merchant_account_type,
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate
        }
        
        # Check against all rules
        for rule in fees_data:
            if match_fee_rule(tx_ctx, rule):
                applicable_fee_ids.add(rule['ID'])

    # 6. Output Result
    sorted_ids = sorted(list(applicable_fee_ids))
    print("\nApplicable Fee IDs:")
    print(", ".join(map(str, sorted_ids)))

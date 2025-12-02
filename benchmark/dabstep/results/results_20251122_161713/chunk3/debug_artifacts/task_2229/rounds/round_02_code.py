# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2229
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8980 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS (Robust Data Processing)
# ---------------------------------------------------------
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
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
    return 0.0

def parse_range(range_str):
    """Parses a string range like '100k-1m' or '>5%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip().replace(',', '').replace('%', '')
    multiplier = 1
    if 'k' in s:
        multiplier = 1000
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000
        s = s.replace('m', '')
        
    if '>' in s:
        return float(s.replace('>', '')) * multiplier, float('inf')
    if '<' in s:
        return float('-inf'), float(s.replace('<', '')) * multiplier
    if '-' in s:
        parts = s.split('-')
        return float(parts[0]) * multiplier, float(parts[1]) * multiplier
    
    try:
        val = float(s) * multiplier
        return val, val
    except:
        return None, None

def match_fee_rule(tx_dict, rule):
    """
    Determines if a transaction matches a specific fee rule.
    tx_dict must contain: card_scheme, is_credit, aci, mcc, account_type, 
                          monthly_volume, monthly_fraud_rate, issuing_country, acquirer_country
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_dict.get('card_scheme'):
        return False

    # 2. Account Type (List check)
    if rule.get('account_type'):
        if tx_dict.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List check)
    if rule.get('merchant_category_code'):
        if tx_dict.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_dict.get('is_credit'):
            return False

    # 5. ACI (List check)
    if rule.get('aci'):
        if tx_dict.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Boolean)
    # Defined as issuer_country == acquirer_country
    if rule.get('intracountry') is not None:
        is_intra = (tx_dict.get('issuing_country') == tx_dict.get('acquirer_country'))
        # Rule expects boolean, data might be 0.0/1.0 or bool
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 7. Monthly Volume (Range check)
    if rule.get('monthly_volume'):
        min_vol, max_vol = parse_range(rule['monthly_volume'])
        vol = tx_dict.get('monthly_volume', 0)
        if not (min_vol <= vol <= max_vol):
            return False

    # 8. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level'):
        min_fraud, max_fraud = parse_range(rule['monthly_fraud_level'])
        # Fraud level in rule is usually %, e.g., "8.3%" -> 8.3
        # Input fraud rate is usually 0-100 scale or 0-1. Let's standardize to 0-100 scale for comparison if rule has %
        fraud = tx_dict.get('monthly_fraud_rate', 0)
        
        # If parse_range handled %, it stripped it. 
        # If rule was "8.3%", min_fraud is 8.3.
        # If our calculated fraud is 0.083 (ratio), we need to multiply by 100.
        # If our calculated fraud is 8.3 (percent), we leave it.
        # Based on standard helper usage, let's assume input is percentage (0-100).
        
        if not (min_fraud <= fraud <= max_fraud):
            return False

    return True

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------
def calculate_fee_delta():
    # 1. Load Data
    print("Loading data...")
    payments = pd.read_csv('/output/chunk3/data/context/payments.csv')
    with open('/output/chunk3/data/context/fees.json', 'r') as f:
        fees = json.load(f)
    with open('/output/chunk3/data/context/merchant_data.json', 'r') as f:
        merchant_data = json.load(f)

    # 2. Define Constants
    TARGET_MERCHANT = 'Golfclub_Baron_Friso'
    TARGET_FEE_ID = 595
    NEW_RATE = 1
    YEAR = 2023
    # December is usually days 335-365 in a non-leap year
    START_DAY = 335
    END_DAY = 365

    # 3. Get Merchant Details (MCC, Account Type)
    merchant_info = next((m for m in merchant_data if m['merchant'] == TARGET_MERCHANT), None)
    if not merchant_info:
        print(f"Error: Merchant {TARGET_MERCHANT} not found in merchant_data.json")
        return

    mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    print(f"Merchant Info: MCC={mcc}, Account Type={account_type}")

    # 4. Filter Payments for Merchant and December 2023
    # We need the WHOLE month to calculate volume/fraud stats correctly
    df_dec = payments[
        (payments['merchant'] == TARGET_MERCHANT) &
        (payments['year'] == YEAR) &
        (payments['day_of_year'] >= START_DAY) &
        (payments['day_of_year'] <= END_DAY)
    ].copy()
    
    if df_dec.empty:
        print("No transactions found for this merchant in December 2023.")
        return

    # 5. Calculate Monthly Stats (Required for Fee Rules)
    # Volume in EUR
    monthly_volume = df_dec['eur_amount'].sum()
    
    # Fraud Rate (as percentage 0-100)
    # "Fraud is defined as the ratio of fraudulent volume over total volume" or count?
    # Manual says: "Fraud is defined as the ratio of fraudulent volume over total volume." (Section 7)
    # However, usually in these datasets, simple count ratio is often used if volume isn't specified.
    # Let's check Section 5: "monthly_fraud_level... ratio between monthly total volume and monthly volume notified as fraud."
    # Okay, it is VOLUME based.
    
    fraud_volume = df_dec[df_dec['has_fraudulent_dispute']]['eur_amount'].sum()
    monthly_fraud_rate = (fraud_volume / monthly_volume * 100) if monthly_volume > 0 else 0.0

    print(f"Monthly Volume: €{monthly_volume:,.2f}")
    print(f"Monthly Fraud Rate: {monthly_fraud_rate:.4f}%")

    # 6. Get the Specific Fee Rule
    fee_rule = next((f for f in fees if f['ID'] == TARGET_FEE_ID), None)
    if not fee_rule:
        print(f"Error: Fee ID {TARGET_FEE_ID} not found.")
        return
    
    print(f"Fee Rule {TARGET_FEE_ID}: {fee_rule}")

    # 7. Identify Matching Transactions
    # We iterate through transactions and check if they match Rule 595
    matching_amounts = []
    
    for _, tx in df_dec.iterrows():
        # Construct transaction dictionary for the matcher
        tx_context = {
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'issuing_country': tx['issuing_country'],
            'acquirer_country': tx['acquirer_country'],
            'eur_amount': tx['eur_amount'],
            # Merchant context
            'mcc': mcc,
            'account_type': account_type,
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate
        }
        
        if match_fee_rule(tx_context, fee_rule):
            matching_amounts.append(tx['eur_amount'])

    # 8. Calculate Delta
    # Formula: Fee = Fixed + (Rate * Amount / 10000)
    # Delta = New_Fee - Old_Fee
    # Delta = (Fixed + New_Rate*Amt/10000) - (Fixed + Old_Rate*Amt/10000)
    # Delta = (New_Rate - Old_Rate) * Amt / 10000
    
    if not matching_amounts:
        print("No transactions matched Fee ID 595.")
        print("0.00000000000000")
        return

    total_matching_volume = sum(matching_amounts)
    old_rate = fee_rule['rate']
    
    # Calculate Delta
    # Note: Rate is an integer (e.g., 19), divided by 10000 in the formula.
    delta = (NEW_RATE - old_rate) * total_matching_volume / 10000.0
    
    print(f"\nMatching Transactions: {len(matching_amounts)}")
    print(f"Total Matching Volume: €{total_matching_volume:,.2f}")
    print(f"Old Rate: {old_rate}")
    print(f"New Rate: {NEW_RATE}")
    print(f"Delta Calculation: ({NEW_RATE} - {old_rate}) * {total_matching_volume} / 10000")
    
    # Output with high precision as requested for delta calculations
    print(f"\nFinal Delta: {delta:.14f}")

if __name__ == "__main__":
    calculate_fee_delta()

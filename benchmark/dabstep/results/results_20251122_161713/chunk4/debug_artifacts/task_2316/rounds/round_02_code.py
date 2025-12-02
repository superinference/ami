# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2316
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8288 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

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
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except ValueError:
                pass
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_volume_range(range_str):
    """Parses volume strings like '100k-1m', '>10m', '<50k' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    multiplier = 1
    
    # Helper to parse single value with k/m suffix
    def parse_val(val_s):
        val_s = val_s.strip()
        mult = 1
        if 'k' in val_s:
            mult = 1000
            val_s = val_s.replace('k', '')
        elif 'm' in val_s:
            mult = 1_000_000
            val_s = val_s.replace('m', '')
        return float(val_s) * mult

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif '>' in s:
        val = parse_val(s.replace('>', ''))
        return val, float('inf')
    elif '<' in s:
        val = parse_val(s.replace('<', ''))
        return 0.0, val
    else:
        # Exact match or malformed
        try:
            val = parse_val(s)
            return val, val
        except:
            return None, None

def parse_fraud_range(range_str):
    """Parses fraud strings like '7.7%-8.3%', '>8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().replace('%', '')
    
    if '-' in s:
        parts = s.split('-')
        return float(parts[0])/100, float(parts[1])/100
    elif '>' in s:
        return float(s.replace('>', ''))/100, 1.0
    elif '<' in s:
        return 0.0, float(s.replace('<', ''))/100
    else:
        try:
            val = float(s)/100
            return val, val
        except:
            return None, None

def match_fee_rule(tx_context, rule):
    """
    Checks if a transaction context matches a fee rule.
    tx_context: dict containing transaction and merchant details
    rule: dict containing fee rule criteria
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match)
    # If rule['account_type'] is empty/null, it applies to all.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match)
    # If rule['is_credit'] is None, applies to both.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean/Float match)
    # Rule might use 0.0/1.0 or boolean.
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['is_intracountry']:
            return False

    # 7. Capture Delay (Exact match)
    if rule.get('capture_delay'):
        if rule['capture_delay'] != tx_context['capture_delay']:
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_volume_range(rule['monthly_volume'])
        if min_v is not None:
            vol = tx_context['monthly_volume']
            if not (min_v <= vol <= max_v):
                return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_fraud_range(rule['monthly_fraud_level'])
        if min_f is not None:
            fraud = tx_context['monthly_fraud_rate']
            # Handle floating point precision slightly loosely if needed, but strict usually fine
            if not (min_f <= fraud <= max_f):
                return False

    return True

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

def main():
    # 1. Load Data
    try:
        payments = pd.read_csv('/output/chunk4/data/context/payments.csv')
        with open('/output/chunk4/data/context/fees.json', 'r') as f:
            fees = json.load(f)
        with open('/output/chunk4/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Filter Data for Martinis_Fine_Steakhouse in September 2023
    target_merchant = "Martinis_Fine_Steakhouse"
    target_year = 2023
    # September 2023 (non-leap): Days 244 to 273
    start_day = 244
    end_day = 273

    df_merchant = payments[
        (payments['merchant'] == target_merchant) &
        (payments['year'] == target_year) &
        (payments['day_of_year'] >= start_day) &
        (payments['day_of_year'] <= end_day)
    ].copy()

    if df_merchant.empty:
        print("0.00000000000000")
        return

    # 3. Calculate Monthly Stats (Volume & Fraud Rate)
    # These stats are required to check if the fee rule applies to this merchant this month
    monthly_volume = df_merchant['eur_amount'].sum()
    
    fraud_count = df_merchant['has_fraudulent_dispute'].sum()
    total_count = len(df_merchant)
    monthly_fraud_rate = fraud_count / total_count if total_count > 0 else 0.0

    # 4. Get Merchant Metadata
    m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_info:
        print("Error: Merchant metadata not found.")
        return

    # 5. Get Fee Rule ID=16
    rule_16 = next((r for r in fees if r['ID'] == 16), None)
    if not rule_16:
        print("Error: Fee ID 16 not found.")
        return

    old_rate = rule_16['rate']
    new_rate = 99

    # 6. Identify Matching Transactions and Calculate Affected Volume
    affected_volume = 0.0

    for _, tx in df_merchant.iterrows():
        # Determine Intracountry status
        # Intracountry is True if Issuer Country == Acquirer Country
        is_intra = (tx['issuing_country'] == tx['acquirer_country'])

        # Build context for matching
        tx_context = {
            'card_scheme': tx['card_scheme'],
            'account_type': m_info['account_type'],
            'mcc': m_info['merchant_category_code'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'is_intracountry': is_intra,
            'capture_delay': m_info['capture_delay'],
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate
        }

        # Check if Rule 16 applies to this transaction
        if match_fee_rule(tx_context, rule_16):
            affected_volume += tx['eur_amount']

    # 7. Calculate Delta
    # Fee = Fixed + (Rate * Amount / 10000)
    # Delta = (New_Fee - Old_Fee)
    #       = (Fixed + New_Rate*Amt/10000) - (Fixed + Old_Rate*Amt/10000)
    #       = (New_Rate - Old_Rate) * Amt / 10000
    
    delta = (new_rate - old_rate) * affected_volume / 10000

    # Print with high precision as requested for delta calculations
    print(f"{delta:.14f}")

if __name__ == "__main__":
    main()

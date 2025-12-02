import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
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

def parse_volume_string(vol_str):
    """Parses volume strings like '100k-1m' into (min, max) tuple."""
    if not vol_str:
        return None
    
    def parse_val(s):
        s = s.lower().strip()
        mult = 1
        if 'k' in s:
            mult = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mult = 1_000_000
            s = s.replace('m', '')
        try:
            return float(s) * mult
        except:
            return 0.0

    try:
        if '-' in vol_str:
            parts = vol_str.split('-')
            return (parse_val(parts[0]), parse_val(parts[1]))
        elif '>' in vol_str:
            val = parse_val(vol_str.replace('>', ''))
            return (val, float('inf'))
        elif '<' in vol_str:
            val = parse_val(vol_str.replace('<', ''))
            return (0, val)
    except:
        return None
    return None

def parse_fraud_string(fraud_str):
    """Parses fraud strings like '7.7%-8.3%' into (min, max) tuple."""
    if not fraud_str:
        return None
    
    def parse_pct(s):
        s = s.strip().replace('%', '')
        try:
            return float(s) / 100.0
        except:
            return 0.0

    try:
        if '-' in fraud_str:
            parts = fraud_str.split('-')
            return (parse_pct(parts[0]), parse_pct(parts[1]))
        elif '>' in fraud_str:
            val = parse_pct(fraud_str.replace('>', ''))
            return (val, float('inf'))
        elif '<' in fraud_str:
            val = parse_pct(fraud_str.replace('<', ''))
            return (0, val)
    except:
        return None
    return None

def check_capture_delay(merchant_delay, rule_delay):
    """Matches merchant capture delay against rule criteria."""
    if rule_delay is None:
        return True
    
    # Normalize inputs
    md = str(merchant_delay).lower().strip()
    rd = str(rule_delay).lower().strip()
    
    if rd == 'immediate':
        return md == 'immediate'
    if rd == 'manual':
        return md == 'manual'
    
    # Numeric checks
    try:
        # If merchant delay is numeric (e.g., "1")
        if md.isdigit():
            md_val = int(md)
            if '-' in rd:
                low, high = map(int, rd.split('-'))
                return low <= md_val <= high
            if rd.startswith('>'):
                return md_val > int(rd[1:])
            if rd.startswith('<'):
                return md_val < int(rd[1:])
    except:
        pass
        
    return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    """
    
    # 1. Card Scheme (Exact match required)
    if rule.get('card_scheme') != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match or Wildcard)
    if rule.get('account_type') and len(rule['account_type']) > 0:
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match or Wildcard)
    if rule.get('merchant_category_code') and len(rule['merchant_category_code']) > 0:
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match or Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 5. ACI (List match or Wildcard)
    if rule.get('aci') and len(rule['aci']) > 0:
        if tx_context['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match or Wildcard)
    # Intracountry: Issuer Country == Acquirer Country
    is_intra = (tx_context['issuing_country'] == tx_context['acquirer_country'])
    if rule.get('intracountry') is not None:
        # rule['intracountry'] is 0.0 or 1.0 in JSON
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 7. Capture Delay (Complex match or Wildcard)
    if rule.get('capture_delay'):
        if not check_capture_delay(tx_context['capture_delay'], rule['capture_delay']):
            return False

    # 8. Monthly Volume (Range match or Wildcard)
    if rule.get('monthly_volume'):
        vol_range = parse_volume_string(rule['monthly_volume'])
        if vol_range:
            if not (vol_range[0] <= tx_context['monthly_volume'] < vol_range[1]):
                return False

    # 9. Monthly Fraud Level (Range match or Wildcard)
    if rule.get('monthly_fraud_level'):
        fraud_range = parse_fraud_string(rule['monthly_fraud_level'])
        if fraud_range:
            # tx_context['monthly_fraud_rate'] is a ratio (e.g. 0.08)
            # fraud_range is (0.077, 0.083)
            if not (fraud_range[0] <= tx_context['monthly_fraud_rate'] <= fraud_range[1]):
                return False

    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Get Merchant Metadata
target_merchant = 'Martinis_Fine_Steakhouse'
merchant_info = next((item for item in merchant_data if item["merchant"] == target_merchant), None)

if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
else:
    # 3. Calculate Monthly Stats for December 2023
    # December is days 335 to 365 (inclusive) in a non-leap year
    dec_start = 335
    dec_end = 365
    
    df_december = df_payments[
        (df_payments['merchant'] == target_merchant) &
        (df_payments['year'] == 2023) &
        (df_payments['day_of_year'] >= dec_start) &
        (df_payments['day_of_year'] <= dec_end)
    ]
    
    monthly_volume = df_december['eur_amount'].sum()
    
    # Fraud Volume / Total Volume
    fraud_volume = df_december[df_december['has_fraudulent_dispute']]['eur_amount'].sum()
    
    if monthly_volume > 0:
        monthly_fraud_rate = fraud_volume / monthly_volume
    else:
        monthly_fraud_rate = 0.0

    # 4. Filter Transactions for Day 365
    df_target_day = df_payments[
        (df_payments['merchant'] == target_merchant) &
        (df_payments['year'] == 2023) &
        (df_payments['day_of_year'] == 365)
    ]

    # 5. Calculate Fees
    total_fees = 0.0
    
    # Pre-prepare context that doesn't change per transaction
    base_context = {
        'account_type': merchant_info['account_type'],
        'merchant_category_code': merchant_info['merchant_category_code'],
        'capture_delay': merchant_info['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_rate': monthly_fraud_rate
    }

    for _, tx in df_target_day.iterrows():
        # Build full context for this transaction
        tx_context = base_context.copy()
        tx_context['card_scheme'] = tx['card_scheme']
        tx_context['is_credit'] = tx['is_credit']
        tx_context['aci'] = tx['aci']
        tx_context['issuing_country'] = tx['issuing_country']
        tx_context['acquirer_country'] = tx['acquirer_country']
        
        # Find matching rule
        matched_rule = None
        # Iterate through rules in order. The problem doesn't specify priority, 
        # but typically in rule engines, the first match or most specific match wins.
        # Given the structure, we'll assume the dataset is ordered or first match is sufficient.
        for rule in fees_data:
            if match_fee_rule(tx_context, rule):
                matched_rule = rule
                break 
        
        if matched_rule:
            # Fee = Fixed + (Rate * Amount / 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000.0)
            total_fees += fee
        else:
            # If no rule matches, we skip or log. 
            # In this context, we assume coverage.
            pass

    print(f"{total_fees:.2f}")
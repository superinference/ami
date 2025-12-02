import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None or pd.isna(value):
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

def parse_volume_range(range_str):
    """Parses volume range strings like '100k-1m', '>10m', '<10k' into (min, max)."""
    if not range_str or pd.isna(range_str) or range_str == 'None':
        return (float('-inf'), float('inf'))
    
    s = str(range_str).lower().replace(',', '').replace('€', '').strip()
    
    def parse_val(val_str):
        m = 1
        if 'k' in val_str:
            m = 1000
            val_str = val_str.replace('k', '')
        elif 'm' in val_str:
            m = 1000000
            val_str = val_str.replace('m', '')
        try:
            return float(val_str) * m
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return (parse_val(parts[0]), parse_val(parts[1]))
    elif '>' in s:
        return (parse_val(s.replace('>', '')), float('inf'))
    elif '<' in s:
        return (float('-inf'), parse_val(s.replace('<', '')))
    else:
        try:
            val = parse_val(s)
            return (val, val)
        except:
            return (float('-inf'), float('inf'))

def parse_fraud_range(range_str):
    """Parses fraud range strings like '0.0%-0.1%', '>8.3%' into (min, max)."""
    if not range_str or pd.isna(range_str) or range_str == 'None':
        return (float('-inf'), float('inf'))
    
    s = str(range_str).replace('%', '').strip()
    
    def to_ratio(val):
        try:
            return float(val) / 100
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return (to_ratio(parts[0]), to_ratio(parts[1]))
    elif '>' in s:
        return (to_ratio(s.replace('>', '')), float('inf'))
    elif '<' in s:
        return (float('-inf'), to_ratio(s.replace('<', '')))
    else:
        try:
            val = to_ratio(s)
            return (val, val)
        except:
            return (float('-inf'), float('inf'))

def match_fee_rule(tx_context, rule):
    """
    Checks if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and tx_context.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (Wildcard: [] or None)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (Wildcard: [] or None)
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 4. Capture Delay
    if rule.get('capture_delay'):
        r_cd = rule['capture_delay']
        m_cd = str(tx_context['capture_delay'])
        
        match = False
        if r_cd == m_cd:
            match = True
        elif r_cd == 'immediate' and m_cd == 'immediate':
            match = True
        elif r_cd == 'manual' and m_cd == 'manual':
            match = True
        elif r_cd == '<3':
            if m_cd.isdigit() and int(m_cd) < 3: match = True
        elif r_cd == '3-5':
            if m_cd.isdigit() and 3 <= int(m_cd) <= 5: match = True
        elif r_cd == '>5':
            if m_cd.isdigit() and int(m_cd) > 5: match = True
            
        if not match:
            return False

    # 5. Monthly Volume
    if rule.get('monthly_volume'):
        min_v, max_v = parse_volume_range(rule['monthly_volume'])
        if not (min_v <= tx_context['monthly_volume'] <= max_v):
            return False

    # 6. Monthly Fraud Level
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_fraud_range(rule['monthly_fraud_level'])
        # Allow small float tolerance
        if not (min_f <= tx_context['monthly_fraud_level'] + 1e-9 and tx_context['monthly_fraud_level'] - 1e-9 <= max_f):
            return False

    # 7. Is Credit
    if rule.get('is_credit') is not None:
        # Handle string 'True'/'False' if present in JSON, though usually bool
        rule_credit = rule['is_credit']
        if str(rule_credit).lower() == 'true': rule_credit = True
        elif str(rule_credit).lower() == 'false': rule_credit = False
        
        if rule_credit != tx_context['is_credit']:
            return False

    # 8. ACI (Wildcard: [] or None)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 9. Intracountry
    if rule.get('intracountry') is not None:
        # fees.json uses 0.0/1.0 or boolean.
        try:
            rule_intra = bool(float(rule['intracountry']))
        except:
            rule_intra = rule['intracountry']
            
        if rule_intra != tx_context['intracountry']:
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee for a given amount and rule."""
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000)

# ═══════════════════════════════════════════════════════════
# MAIN ANALYSIS
# ═══════════════════════════════════════════════════════════

def main():
    # 1. Load Data
    try:
        df_payments = pd.read_csv('/output/chunk2/data/context/payments.csv')
        with open('/output/chunk2/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
        with open('/output/chunk2/data/context/fees.json', 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Determine Average Scenario Characteristics
    # Merchant (Mode)
    mode_merchant = df_payments['merchant'].mode()[0]
    
    # Is Credit (Mode)
    mode_is_credit = df_payments['is_credit'].mode()[0]
    
    # ACI (Mode)
    mode_aci = df_payments['aci'].mode()[0]
    
    # Intracountry (Mode)
    # Intracountry is defined as issuing_country == acquirer_country
    df_payments['is_intracountry'] = df_payments['issuing_country'] == df_payments['acquirer_country']
    mode_intracountry = df_payments['is_intracountry'].mode()[0]

    # 3. Get Merchant Metadata
    merchant_info = next((m for m in merchant_data if m['merchant'] == mode_merchant), None)
    if not merchant_info:
        print(f"Merchant {mode_merchant} not found in merchant_data.json")
        return

    # 4. Calculate Merchant Monthly Stats
    # Filter for this merchant
    merchant_txs = df_payments[df_payments['merchant'] == mode_merchant]
    
    # Monthly Volume: Total Volume / 12 (assuming 1 year data)
    total_volume = merchant_txs['eur_amount'].sum()
    monthly_volume = total_volume / 12.0
    
    # Monthly Fraud Level: Fraud Volume / Total Volume
    fraud_txs = merchant_txs[merchant_txs['has_fraudulent_dispute'] == True]
    fraud_volume = fraud_txs['eur_amount'].sum()
    monthly_fraud_level = fraud_volume / total_volume if total_volume > 0 else 0.0

    # 5. Prepare Context for Matching
    context = {
        'merchant_category_code': merchant_info['merchant_category_code'],
        'account_type': merchant_info['account_type'],
        'capture_delay': merchant_info['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level,
        'is_credit': bool(mode_is_credit),
        'aci': mode_aci,
        'intracountry': bool(mode_intracountry)
    }

    # 6. Evaluate Fees for Each Scheme
    schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
    transaction_amount = 100.0
    
    cheapest_scheme = None
    min_fee = float('inf')
    
    results = {}

    for scheme in schemes:
        context['card_scheme'] = scheme
        
        # Find matching rules
        matching_rules = []
        for rule in fees_data:
            if match_fee_rule(context, rule):
                matching_rules.append(rule)
        
        if not matching_rules:
            continue
            
        # Calculate fee for ALL matching rules and take the MINIMUM valid fee
        # (Assuming the merchant gets the best applicable rate if multiple rules overlap)
        scheme_fees = []
        for rule in matching_rules:
            fee = calculate_fee(transaction_amount, rule)
            scheme_fees.append(fee)
        
        if scheme_fees:
            best_scheme_fee = min(scheme_fees)
            results[scheme] = best_scheme_fee
            
            if best_scheme_fee < min_fee:
                min_fee = best_scheme_fee
                cheapest_scheme = scheme

    # 7. Output Result
    # The question asks "which card scheme", so we print the name.
    print(cheapest_scheme)

if __name__ == "__main__":
    main()
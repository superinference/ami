# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1281
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 7982 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v:
            return float(v.replace('k', '')) * 1000
        if 'm' in v:
            return float(v.replace('m', '')) * 1000000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range(range_str):
    """Parses strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip()
    
    if '-' in s:
        parts = s.split('-')
        min_val = coerce_to_float(parts[0])
        max_val = coerce_to_float(parts[1])
        return min_val, max_val
    elif s.startswith('>'):
        val = coerce_to_float(s[1:])
        return val, float('inf')
    elif s.startswith('<'):
        val = coerce_to_float(s[1:])
        return float('-inf'), val
    else:
        # Exact match treated as range [val, val]
        val = coerce_to_float(s)
        return val, val

def check_rule_match(context, rule):
    """
    Checks if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != context['card_scheme']:
        return False

    # 2. Is Credit (Exact match, handle null as wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != context['is_credit']:
            return False

    # 3. Account Type (List match, empty = wildcard)
    if rule.get('account_type'): 
        if context['account_type'] not in rule['account_type']:
            return False

    # 4. Merchant Category Code (List match, empty = wildcard)
    if rule.get('merchant_category_code'): 
        if context['mcc'] not in rule['merchant_category_code']:
            return False

    # 5. ACI (List match, empty = wildcard)
    if rule.get('aci'): 
        if context['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match, null = wildcard)
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != context['intracountry']:
            return False

    # 7. Capture Delay (String match or Range, null = wildcard)
    if rule.get('capture_delay') is not None:
        r_delay = str(rule['capture_delay'])
        c_delay = str(context['capture_delay'])
        
        # Direct string match (e.g. 'manual' == 'manual')
        if r_delay == c_delay:
            pass 
        # Numeric comparison if rule is range
        elif any(x in r_delay for x in ['<', '>', '-']):
            try:
                c_val = float(c_delay)
                min_d, max_d = parse_range(r_delay)
                if not (min_d <= c_val <= max_d):
                    return False
            except ValueError:
                # Context is 'manual'/'immediate' but rule is numeric range -> No match
                return False
        else:
            # Rule is specific value but didn't match string equality
            return False

    # 8. Monthly Volume (Range match, null = wildcard)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= context['monthly_volume'] <= max_v):
            return False

    # 9. Monthly Fraud Level (Range match, null = wildcard)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if not (min_f <= context['monthly_fraud_level'] <= max_f):
            return False

    return True

# --- Main Execution ---
def main():
    # 1. Load Data
    payments_path = '/output/chunk3/data/context/payments.csv'
    merchant_data_path = '/output/chunk3/data/context/merchant_data.json'
    fees_path = '/output/chunk3/data/context/fees.json'

    df_payments = pd.read_csv(payments_path)
    with open(merchant_data_path, 'r') as f:
        merchant_data = json.load(f)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)

    # Create merchant lookup dict
    merchant_lookup = {m['merchant']: m for m in merchant_data}

    # 2. Preprocessing: Add Month and Calculate Monthly Stats
    # Convert day_of_year to month (2023)
    df_payments['date'] = pd.to_datetime(df_payments['year'] * 1000 + df_payments['day_of_year'], format='%Y%j')
    df_payments['month'] = df_payments['date'].dt.month

    # Calculate monthly volume and fraud rate per merchant
    monthly_stats = df_payments.groupby(['merchant', 'month']).agg(
        monthly_vol=('eur_amount', 'sum'),
        tx_count=('psp_reference', 'count'),
        fraud_count=('has_fraudulent_dispute', 'sum')
    ).reset_index()

    monthly_stats['monthly_fraud_rate'] = monthly_stats['fraud_count'] / monthly_stats['tx_count']

    # Create stats lookup: (merchant, month) -> {vol, fraud}
    stats_map = {}
    for _, row in monthly_stats.iterrows():
        stats_map[(row['merchant'], row['month'])] = {
            'vol': row['monthly_vol'],
            'fraud': row['monthly_fraud_rate']
        }

    # 3. Filter Target Transactions
    # GlobalCard, Credit
    target_df = df_payments[
        (df_payments['card_scheme'] == 'GlobalCard') & 
        (df_payments['is_credit'] == True)
    ].copy()

    # 4. Optimization: Pre-filter fees for GlobalCard and Credit=True
    # This significantly speeds up the matching process
    relevant_fees = []
    for rule in fees_data:
        # Scheme match
        if rule.get('card_scheme') and rule['card_scheme'] != 'GlobalCard':
            continue
        # Credit match (Target is True)
        # If rule is False, it doesn't match. If None or True, it matches.
        if rule.get('is_credit') is not None and rule['is_credit'] is False:
            continue
        relevant_fees.append(rule)
    
    # Sort relevant fees by ID (Priority)
    relevant_fees.sort(key=lambda x: x['ID'])

    # 5. Calculate Fees for Hypothetical 100 EUR
    calculated_fees = []
    hypothetical_amount = 100.0

    for _, tx in target_df.iterrows():
        merchant = tx['merchant']
        month = tx['month']
        
        # Retrieve Context Data
        m_info = merchant_lookup.get(merchant)
        m_stats = stats_map.get((merchant, month))
        
        if not m_info or not m_stats:
            continue
            
        # Build Context
        context = {
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'],
            'account_type': m_info['account_type'],
            'mcc': m_info['merchant_category_code'],
            'aci': tx['aci'],
            'intracountry': tx['issuing_country'] == tx['acquirer_country'],
            'capture_delay': m_info['capture_delay'],
            'monthly_volume': m_stats['vol'],
            'monthly_fraud_level': m_stats['fraud']
        }
        
        # Find First Matching Rule
        matched_rule = None
        for rule in relevant_fees:
            if check_rule_match(context, rule):
                matched_rule = rule
                break
                
        if matched_rule:
            # Calculate Fee
            # Formula: fixed + (rate * amount / 10000)
            fixed = matched_rule['fixed_amount']
            rate = matched_rule['rate']
            
            fee = fixed + (rate * hypothetical_amount / 10000.0)
            calculated_fees.append(fee)

    # 6. Compute Average
    if calculated_fees:
        average_fee = sum(calculated_fees) / len(calculated_fees)
        print(f"{average_fee:.14f}")
    else:
        print("0.00")

if __name__ == "__main__":
    main()

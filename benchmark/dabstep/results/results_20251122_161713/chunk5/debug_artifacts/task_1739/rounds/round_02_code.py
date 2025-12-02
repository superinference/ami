# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1739
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9187 characters (FULL CODE)
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
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                # Check if parts are numbers
                p1 = parts[0].replace('%', '').strip()
                p2 = parts[1].replace('%', '').strip()
                return (float(p1) + float(p2)) / 2
            except:
                pass
        return float(v)
    return float(value)

def parse_range_string(range_str, value, is_percentage=False):
    """
    Parses range strings like '100k-1m', '<3', '>5', '0.0%-0.5%' and checks if value is in range.
    value: The calculated numeric value to check.
    is_percentage: If True, treats value as 0-100 scale if range has %, or 0-1 if range is decimal.
                   Actually, let's standardize: value passed in should be comparable to the parsed range.
                   If range is '8.3%', it parses to 8.3. So value should be 8.3 (not 0.083).
    """
    if range_str is None:
        return True
    
    s = str(range_str).strip().lower()
    
    # Handle k/m suffixes
    def parse_num(n_str):
        n_str = n_str.replace('%', '')
        if 'k' in n_str:
            return float(n_str.replace('k', '')) * 1000
        if 'm' in n_str:
            return float(n_str.replace('m', '')) * 1000000
        return float(n_str)

    try:
        if '-' in s:
            parts = s.split('-')
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            return low <= value <= high
        elif s.startswith('>'):
            limit = parse_num(s[1:])
            return value > limit
        elif s.startswith('<'):
            limit = parse_num(s[1:])
            return value < limit
        elif s == 'immediate':
            # Special case for capture_delay, not numeric
            return False 
        else:
            # Exact match numeric string
            return value == parse_num(s)
    except ValueError:
        # Fallback for non-numeric strings (like 'manual')
        return str(value).lower() == s

def match_fee_rule(tx_context, rule):
    """
    Checks if a fee rule applies to a transaction context.
    tx_context: dict containing transaction and merchant details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match or wildcard)
    if rule['account_type']: # If not empty/null
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match or wildcard)
    if rule['merchant_category_code']: # If not empty/null
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match or wildcard)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 5. ACI (List match or wildcard)
    if rule['aci']: # If not empty/null
        if tx_context['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match or wildcard)
    if rule['intracountry'] is not None:
        # Convert rule value to bool if it's 0.0/1.0
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['is_intracountry']:
            return False

    # 7. Capture Delay (String match or wildcard)
    if rule['capture_delay'] is not None:
        # capture_delay in merchant_data is string (e.g. "manual", "immediate", "1")
        # rule['capture_delay'] is string (e.g. "manual", ">5", "<3")
        # We need to handle the numeric comparison if the rule is a range
        m_delay = str(tx_context['capture_delay'])
        r_delay = str(rule['capture_delay'])
        
        if r_delay in ['manual', 'immediate']:
            if m_delay != r_delay:
                return False
        elif m_delay in ['manual', 'immediate']:
             if m_delay != r_delay:
                return False
        else:
            # Both are likely numeric
            try:
                val = float(m_delay)
                if not parse_range_string(r_delay, val):
                    return False
            except:
                if m_delay != r_delay:
                    return False

    # 8. Monthly Volume (Range match or wildcard)
    if rule['monthly_volume'] is not None:
        if not parse_range_string(rule['monthly_volume'], tx_context['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match or wildcard)
    if rule['monthly_fraud_level'] is not None:
        # Fraud level in context is percentage (0-100)
        if not parse_range_string(rule['monthly_fraud_level'], tx_context['monthly_fraud_rate']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    # fee = fixed + (rate * amount / 10000)
    # rate is an integer, e.g., 19 means 0.0019 multiplier
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN ANALYSIS
# ═══════════════════════════════════════════════════════════

def main():
    # Paths
    payments_path = '/output/chunk5/data/context/payments.csv'
    fees_path = '/output/chunk5/data/context/fees.json'
    merchant_path = '/output/chunk5/data/context/merchant_data.json'

    # 1. Load Data
    print("Loading data...")
    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)

    # 2. Filter for Rafa_AI
    target_merchant = 'Rafa_AI'
    df_rafa = df_payments[df_payments['merchant'] == target_merchant].copy()
    
    if df_rafa.empty:
        print(f"No transactions found for {target_merchant}")
        return

    # 3. Calculate Monthly Stats for October (Days 274-304)
    # October in non-leap year:
    # Jan(31)+Feb(28)+Mar(31)+Apr(30)+May(31)+Jun(30)+Jul(31)+Aug(31)+Sep(30) = 273 days
    # Oct starts day 274, ends 274+31-1 = 304
    oct_start = 274
    oct_end = 304
    
    df_oct = df_rafa[(df_rafa['day_of_year'] >= oct_start) & (df_rafa['day_of_year'] <= oct_end)]
    
    monthly_volume = df_oct['eur_amount'].sum()
    fraud_count = df_oct['has_fraudulent_dispute'].sum()
    total_count = len(df_oct)
    monthly_fraud_rate = (fraud_count / total_count * 100) if total_count > 0 else 0.0

    print(f"October Stats for {target_merchant}:")
    print(f"  Volume: €{monthly_volume:,.2f}")
    print(f"  Fraud Rate: {monthly_fraud_rate:.4f}% ({fraud_count}/{total_count})")

    # 4. Get Merchant Static Data
    m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_info:
        print(f"Merchant info not found for {target_merchant}")
        return

    # 5. Filter for Day 300
    target_day = 300
    df_day_300 = df_rafa[df_rafa['day_of_year'] == target_day]
    print(f"Transactions on Day {target_day}: {len(df_day_300)}")

    # 6. Calculate Fees
    total_fees = 0.0
    
    # Sort fees by ID to ensure deterministic matching order (usually lower ID = higher priority or default order)
    fees_data.sort(key=lambda x: x['ID'])

    for idx, tx in df_day_300.iterrows():
        # Build context for matching
        tx_context = {
            'card_scheme': tx['card_scheme'],
            'account_type': m_info['account_type'],
            'mcc': m_info['merchant_category_code'],
            'is_credit': bool(tx['is_credit']),
            'aci': tx['aci'],
            'is_intracountry': tx['issuing_country'] == tx['acquirer_country'],
            'capture_delay': m_info['capture_delay'],
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate
        }

        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(tx_context, rule):
                matched_rule = rule
                break # Stop at first match
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fees += fee
        else:
            print(f"WARNING: No fee rule matched for transaction {tx['psp_reference']}")

    # 7. Output Result
    print(f"\nTotal fees for {target_merchant} on Day {target_day}: {total_fees:.14f}")
    print(f"{total_fees:.2f}") # Standard output format

if __name__ == "__main__":
    main()

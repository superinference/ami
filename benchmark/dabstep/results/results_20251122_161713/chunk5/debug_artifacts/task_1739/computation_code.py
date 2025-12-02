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
        return float(v)
    return float(value)

def parse_range_check(rule_value, actual_value):
    """
    Checks if actual_value satisfies the rule_value condition.
    rule_value: string from fees.json (e.g., '100k-1m', '>5', 'manual', '0.0%-0.5%') or None
    actual_value: calculated float (volume, rate) or string (capture_delay)
    """
    if rule_value is None:
        return True
    
    # Normalize strings for comparison
    r_str = str(rule_value).strip().lower()
    a_str = str(actual_value).strip().lower()
    
    # 1. Direct string match (handles 'manual' == 'manual', 'immediate' == 'immediate')
    if r_str == a_str:
        return True
        
    # 2. Numeric Logic
    # Helper to parse rule numbers (handles k, m, %)
    def parse_num(n_s):
        n_s = n_s.strip()
        is_pct = '%' in n_s
        # Handle k/m suffixes
        if 'k' in n_s and 'm' not in n_s:
            clean = n_s.replace('k', '').replace('%', '')
            val = float(clean) * 1000
        elif 'm' in n_s:
            clean = n_s.replace('m', '').replace('%', '')
            val = float(clean) * 1000000
        else:
            clean = n_s.replace('%', '')
            val = float(clean)
            
        if is_pct: 
            val /= 100.0
        return val

    try:
        # Attempt to convert actual_value to float
        a_val = float(actual_value)
        
        if '-' in r_str:
            parts = r_str.split('-')
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            # Use a small epsilon for float comparison
            return (low - 1e-9) <= a_val <= (high + 1e-9)
            
        elif r_str.startswith('>'):
            limit = parse_num(r_str[1:])
            return a_val > limit
            
        elif r_str.startswith('<'):
            limit = parse_num(r_str[1:])
            return a_val < limit
            
        else:
            # Try exact numeric match if rule is just a number string like "1"
            target = parse_num(r_str)
            return abs(a_val - target) < 1e-9
            
    except ValueError:
        # actual_value was not a number (e.g. "manual"), but rule was numeric (e.g. ">5")
        # or rule was not parseable.
        # Since we already checked direct string equality, this is a mismatch.
        return False

def match_fee_rule(tx_context, rule):
    """
    Checks if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match or wildcard)
    # Rule: [] means ALL. If not empty, must contain merchant's type.
    if rule['account_type']: 
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match or wildcard)
    # Rule: [] means ALL. If not empty, must contain merchant's MCC.
    if rule['merchant_category_code']: 
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match or wildcard)
    # Rule: null means ALL.
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 5. ACI (List match or wildcard)
    # Rule: [] means ALL. If not empty, must contain transaction's ACI.
    if rule['aci']: 
        if tx_context['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match or wildcard)
    # Rule: null means ALL. 0.0/1.0 means False/True.
    if rule['intracountry'] is not None:
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_context['is_intracountry']:
            return False

    # 7. Capture Delay (String/Range match or wildcard)
    if rule['capture_delay'] is not None:
        if not parse_range_check(rule['capture_delay'], tx_context['capture_delay']):
            return False

    # 8. Monthly Volume (Range match or wildcard)
    if rule['monthly_volume'] is not None:
        if not parse_range_check(rule['monthly_volume'], tx_context['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match or wildcard)
    if rule['monthly_fraud_level'] is not None:
        if not parse_range_check(rule['monthly_fraud_level'], tx_context['monthly_fraud_rate']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    # fee = fixed + (rate * amount / 10000)
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

    target_merchant = 'Rafa_AI'
    
    # 2. Filter for Rafa_AI
    df_rafa = df_payments[df_payments['merchant'] == target_merchant].copy()
    if df_rafa.empty:
        print(f"No transactions found for {target_merchant}")
        return

    # 3. Calculate Monthly Stats for October (Days 274-304)
    # Manual Section 5: "Monthly volumes... computed always in natural months"
    # Manual Section 5: "monthly_fraud_level... measured as ratio between monthly total volume and monthly volume notified as fraud"
    oct_start = 274
    oct_end = 304
    
    df_oct = df_rafa[(df_rafa['day_of_year'] >= oct_start) & (df_rafa['day_of_year'] <= oct_end)]
    
    monthly_volume = df_oct['eur_amount'].sum()
    
    # Fraud Volume: Sum of amounts where has_fraudulent_dispute is True
    fraud_volume = df_oct[df_oct['has_fraudulent_dispute']]['eur_amount'].sum()
    
    # Fraud Rate (Ratio 0.0 - 1.0)
    monthly_fraud_rate = (fraud_volume / monthly_volume) if monthly_volume > 0 else 0.0

    print(f"October Stats for {target_merchant}:")
    print(f"  Total Volume: €{monthly_volume:,.2f}")
    print(f"  Fraud Volume: €{fraud_volume:,.2f}")
    print(f"  Fraud Rate (Vol/Vol): {monthly_fraud_rate:.6f} ({monthly_fraud_rate*100:.4f}%)")

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
    
    # Sort fees by ID to ensure deterministic matching (lower ID usually takes precedence)
    fees_data.sort(key=lambda x: x['ID'])

    match_count = 0
    unmatched_count = 0
    
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
            match_count += 1
        else:
            unmatched_count += 1
            # Debugging unmatched
            # print(f"UNMATCHED: {tx['psp_reference']} Scheme:{tx['card_scheme']} Credit:{tx['is_credit']} ACI:{tx['aci']}")

    # 7. Output Result
    print(f"\nMatched {match_count}/{len(df_day_300)} transactions.")
    if unmatched_count > 0:
        print(f"WARNING: {unmatched_count} transactions did not match any fee rule.")
        
    print(f"Total fees for {target_merchant} on Day {target_day}: {total_fees:.14f}")
    print(f"{total_fees:.2f}")

if __name__ == "__main__":
    main()
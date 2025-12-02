# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1739
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: insufficient
# Plan steps: 2
# Code length: 9315 characters (FULL CODE)
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
                p1 = parts[0].replace('%', '').strip()
                p2 = parts[1].replace('%', '').strip()
                # If percentages, divide by 100
                div = 100 if '%' in value else 1
                return ((float(p1) + float(p2)) / 2) / div
            except:
                pass
        return float(v)
    return float(value)

def parse_range_check(range_str, value):
    """
    Parses range strings like '100k-1m', '<3', '>5', '0.0%-0.5%' and checks if value is in range.
    value: The calculated numeric value to check.
    """
    if range_str is None:
        return True
    
    s = str(range_str).strip().lower()
    
    # Helper to parse numbers with k/m suffixes
    def parse_num(n_str):
        is_pct = '%' in n_str
        clean_str = n_str.replace('%', '').strip()
        if 'k' in clean_str:
            val = float(clean_str.replace('k', '')) * 1000
        elif 'm' in clean_str:
            val = float(clean_str.replace('m', '')) * 1000000
        else:
            val = float(clean_str)
        
        if is_pct:
            return val / 100.0
        return val

    try:
        if '-' in s:
            parts = s.split('-')
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            # Use a small epsilon for float comparison if needed, or direct <=
            return low <= value <= high
        elif s.startswith('>'):
            limit = parse_num(s[1:])
            return value > limit
        elif s.startswith('<'):
            limit = parse_num(s[1:])
            return value < limit
        elif s == 'immediate' or s == 'manual':
            # String match handled in exception or separate check, 
            # but if value is string, we compare here
            return str(value).lower() == s
        else:
            # Exact match numeric
            return value == parse_num(s)
    except ValueError:
        # Fallback for non-numeric strings (like 'manual' vs 'manual')
        return str(value).lower() == s

def match_fee_rule(tx_context, rule):
    """
    Checks if a fee rule applies to a transaction context.
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
        # Convert rule value to bool (0.0 -> False, 1.0 -> True)
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['is_intracountry']:
            return False

    # 7. Capture Delay (String match or wildcard)
    if rule['capture_delay'] is not None:
        # Merchant delay is string (e.g. "manual"), rule is string (e.g. "manual", ">5")
        if not parse_range_check(rule['capture_delay'], tx_context['capture_delay']):
            # Special handling: if merchant has numeric delay (e.g. "1") and rule is range
            try:
                val = float(tx_context['capture_delay'])
                if not parse_range_check(rule['capture_delay'], val):
                    return False
            except ValueError:
                # Merchant delay is non-numeric (e.g. "manual"), rule is range (e.g. ">5") -> False
                # Or rule is "manual" -> True (handled by parse_range_check fallback)
                if str(tx_context['capture_delay']) != str(rule['capture_delay']):
                    return False

    # 8. Monthly Volume (Range match or wildcard)
    if rule['monthly_volume'] is not None:
        if not parse_range_check(rule['monthly_volume'], tx_context['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match or wildcard)
    if rule['monthly_fraud_level'] is not None:
        # Context fraud rate is 0.0 to 1.0 (ratio)
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
            # Fallback or error logging
            pass

    # 7. Output Result
    print(f"\nMatched {match_count}/{len(df_day_300)} transactions.")
    print(f"Total fees for {target_merchant} on Day {target_day}: {total_fees:.14f}")
    print(f"{total_fees:.2f}")

if __name__ == "__main__":
    main()

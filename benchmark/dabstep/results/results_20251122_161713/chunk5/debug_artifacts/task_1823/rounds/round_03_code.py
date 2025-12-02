# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1823
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: insufficient
# Plan steps: 1
# Code length: 7952 characters (FULL CODE)
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
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_range_str(range_str):
    """
    Parses a range string into (min, max).
    Handles 'k' (thousands), 'm' (millions), '%' (percent).
    Returns (None, None) if the string is not a valid numeric range.
    """
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # If no digits, it's likely a categorical string like "manual"
    if not any(c.isdigit() for c in s):
        return None, None

    def parse_val(val_s):
        val_s = val_s.strip()
        multiplier = 1.0
        if val_s.endswith('%'):
            val_s = val_s[:-1]
            multiplier = 0.01
        elif val_s.endswith('k'):
            val_s = val_s[:-1]
            multiplier = 1000.0
        elif val_s.endswith('m'):
            val_s = val_s[:-1]
            multiplier = 1000000.0
        
        val_s = val_s.lstrip('><')
        try:
            return float(val_s) * multiplier
        except ValueError:
            return None

    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            v1 = parse_val(parts[0])
            v2 = parse_val(parts[1])
            if v1 is not None and v2 is not None:
                return v1, v2
    elif s.startswith('>'):
        v = parse_val(s[1:])
        if v is not None:
            return v, float('inf')
    elif s.startswith('<'):
        v = parse_val(s[1:])
        if v is not None:
            return float('-inf'), v
    
    # Single value treated as exact match range
    v = parse_val(s)
    if v is not None:
        return v, v
        
    return None, None

def check_rule_match(tx_ctx, rule):
    """
    Checks if a transaction context matches a fee rule.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List match or Wildcard)
    if rule.get('account_type'): 
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match or Wildcard)
    if rule.get('merchant_category_code'): 
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match or Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 5. ACI (List match or Wildcard)
    if rule.get('aci'): 
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match or Wildcard)
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False

    # 7. Capture Delay (String match OR Range match)
    if rule.get('capture_delay') is not None:
        r_delay = rule['capture_delay']
        t_delay = tx_ctx['capture_delay']
        
        # Exact string match (e.g., "manual" == "manual")
        if str(r_delay).lower() == str(t_delay).lower():
            pass
        else:
            # Range match (e.g., "2" inside "<3")
            min_v, max_v = parse_range_str(r_delay)
            if min_v is not None:
                try:
                    # Merchant delay must be convertible to float to match a range
                    t_val = float(t_delay)
                    if not (min_v <= t_val <= max_v):
                        return False
                except ValueError:
                    # Merchant delay is non-numeric (e.g. "manual") but rule is numeric range
                    return False
            else:
                # Rule is not a range and string didn't match
                return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume') is not None:
        min_v, max_v = parse_range_str(rule['monthly_volume'])
        if min_v is not None:
            if not (min_v <= tx_ctx['monthly_volume'] <= max_v):
                return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level') is not None:
        min_v, max_v = parse_range_str(rule['monthly_fraud_level'])
        if min_v is not None:
            if not (min_v <= tx_ctx['monthly_fraud_level'] <= max_v):
                return False

    return True

def calculate_fee_amount(amount, rule):
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

# ==========================================
# MAIN LOGIC
# ==========================================

def main():
    # File paths
    payments_path = '/output/chunk5/data/context/payments.csv'
    merchant_path = '/output/chunk5/data/context/merchant_data.json'
    fees_path = '/output/chunk5/data/context/fees.json'

    # 1. Load Data
    df = pd.read_csv(payments_path)
    with open(merchant_path, 'r') as f:
        merchants = json.load(f)
    with open(fees_path, 'r') as f:
        fees = json.load(f)

    # 2. Filter for Crossfit_Hanna, Jan 2023
    target_merchant = 'Crossfit_Hanna'
    df_jan = df[
        (df['merchant'] == target_merchant) & 
        (df['year'] == 2023) & 
        (df['day_of_year'] >= 1) & 
        (df['day_of_year'] <= 31)
    ].copy()

    if df_jan.empty:
        print("0.0")
        return

    # 3. Get Merchant Metadata
    m_data = next((m for m in merchants if m['merchant'] == target_merchant), None)
    if not m_data:
        print(f"Error: Merchant {target_merchant} not found")
        return

    # 4. Calculate Monthly Aggregates (Volume & Fraud)
    monthly_volume = df_jan['eur_amount'].sum()
    fraud_volume = df_jan[df_jan['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    
    if monthly_volume > 0:
        monthly_fraud_level = fraud_volume / monthly_volume
    else:
        monthly_fraud_level = 0.0

    # 5. Calculate Fees per Transaction
    total_fees = 0.0
    
    # Static context (same for all txs of this merchant this month)
    static_ctx = {
        'account_type': m_data['account_type'],
        'mcc': m_data['merchant_category_code'],
        'capture_delay': m_data['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }

    # Iterate transactions
    for _, row in df_jan.iterrows():
        # Dynamic context (per transaction)
        ctx = static_ctx.copy()
        ctx['card_scheme'] = row['card_scheme']
        ctx['is_credit'] = row['is_credit']
        ctx['aci'] = row['aci']
        ctx['intracountry'] = (row['issuing_country'] == row['acquirer_country'])
        
        # Find first matching rule
        matched = False
        for rule in fees:
            if check_rule_match(ctx, rule):
                fee = calculate_fee_amount(row['eur_amount'], rule)
                total_fees += fee
                matched = True
                break # Stop after first match
        
        if not matched:
            # If no rule matches, fee is 0 (or handle as error if strict)
            pass

    # Output result
    print(f"{total_fees:.14f}")

if __name__ == "__main__":
    main()

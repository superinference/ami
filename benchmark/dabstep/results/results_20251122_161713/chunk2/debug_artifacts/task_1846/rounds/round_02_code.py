# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1846
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8452 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# Helper Functions for Data Parsing and Rule Matching
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
    return float(value) if value is not None else 0.0

def parse_volume_check(rule_vol_str, actual_vol):
    """Checks if actual volume falls within the rule's volume range string."""
    if not rule_vol_str:  # Wildcard
        return True
        
    def parse_val(s):
        s = s.lower().strip()
        mult = 1
        if 'k' in s:
            mult = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mult = 1000000
            s = s.replace('m', '')
        try:
            return float(s) * mult
        except ValueError:
            return 0.0

    s = str(rule_vol_str).strip()
    if '-' in s:
        low, high = s.split('-')
        return parse_val(low) <= actual_vol <= parse_val(high)
    elif '>' in s:
        val = parse_val(s.replace('>', ''))
        return actual_vol > val
    elif '<' in s:
        val = parse_val(s.replace('<', ''))
        return actual_vol < val
    return False

def parse_fraud_check(rule_fraud_str, actual_rate):
    """Checks if actual fraud rate falls within the rule's fraud range string."""
    if not rule_fraud_str:  # Wildcard
        return True
        
    def parse_pct(s):
        s = s.strip().replace('%', '')
        try:
            return float(s) / 100.0
        except ValueError:
            return 0.0

    s = str(rule_fraud_str).strip()
    if '-' in s:
        low, high = s.split('-')
        return parse_pct(low) <= actual_rate <= parse_pct(high)
    elif '>' in s:
        val = parse_pct(s.replace('>', ''))
        return actual_rate > val
    elif '<' in s:
        val = parse_pct(s.replace('<', ''))
        return actual_rate < val
    return False

def check_capture_delay(rule_val, merchant_val):
    """Matches merchant capture delay against rule (which can be range or value)."""
    if rule_val is None:
        return True
    
    # Direct string match
    if str(rule_val) == str(merchant_val):
        return True
        
    # Numeric comparison for ranges (e.g. merchant="1", rule="<3")
    try:
        m_days = float(merchant_val)
    except ValueError:
        # Merchant value is non-numeric (e.g. "manual"), rule is likely numeric range
        return False
        
    s = str(rule_val).strip()
    if '-' in s:
        try:
            low, high = map(float, s.split('-'))
            return low <= m_days <= high
        except:
            return False
    elif '<' in s:
        try:
            val = float(s.replace('<', ''))
            return m_days < val
        except:
            return False
    elif '>' in s:
        try:
            val = float(s.replace('>', ''))
            return m_days > val
        except:
            return False
            
    return False

def match_fee_rule(ctx, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    ctx: dict containing transaction and merchant details
    rule: dict containing fee rule criteria
    """
    # 1. Card Scheme (Exact match required)
    if rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List contains or Wildcard)
    if rule['account_type'] and ctx['account_type'] not in rule['account_type']:
        return False
        
    # 3. Capture Delay (Complex match or Wildcard)
    if not check_capture_delay(rule['capture_delay'], ctx['capture_delay']):
        return False
        
    # 4. Merchant Category Code (List contains or Wildcard)
    if rule['merchant_category_code'] and ctx['mcc'] not in rule['merchant_category_code']:
        return False
        
    # 5. Is Credit (Bool match or Wildcard)
    if rule['is_credit'] is not None and rule['is_credit'] != ctx['is_credit']:
        return False
        
    # 6. ACI (List contains or Wildcard)
    if rule['aci'] and ctx['aci'] not in rule['aci']:
        return False
        
    # 7. Intracountry (Bool match or Wildcard)
    # rule['intracountry'] is 0.0 (False), 1.0 (True), or None (Wildcard)
    if rule['intracountry'] is not None:
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != ctx['is_intracountry']:
            return False
            
    # 8. Monthly Volume (Range match or Wildcard)
    if not parse_volume_check(rule['monthly_volume'], ctx['monthly_volume']):
        return False
            
    # 9. Monthly Fraud Level (Range match or Wildcard)
    if not parse_fraud_check(rule['monthly_fraud_level'], ctx['monthly_fraud_rate']):
        return False
            
    return True

# ---------------------------------------------------------
# Main Execution Logic
# ---------------------------------------------------------

def main():
    # 1. Load Data
    payments_path = '/output/chunk2/data/context/payments.csv'
    merchant_path = '/output/chunk2/data/context/merchant_data.json'
    fees_path = '/output/chunk2/data/context/fees.json'

    df = pd.read_csv(payments_path)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
    with open(fees_path, 'r') as f:
        fees = json.load(f)

    # 2. Filter for Target Merchant and Time Period
    target_merchant = 'Golfclub_Baron_Friso'
    
    # Get Merchant Profile
    merchant_profile = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_profile:
        print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
        return

    # Filter Payments: Merchant + December 2023 (Day >= 335)
    # Note: 2023 is non-leap. Dec 1 is day 335.
    df_merchant = df[df['merchant'] == target_merchant]
    df_dec = df_merchant[(df_merchant['year'] == 2023) & (df_merchant['day_of_year'] >= 335)].copy()

    if df_dec.empty:
        print("No transactions found for this merchant in December 2023.")
        return

    # 3. Calculate Monthly Stats (Required for Fee Rules)
    # Volume: Sum of eur_amount
    total_volume = df_dec['eur_amount'].sum()
    
    # Fraud Rate: Fraud Volume / Total Volume (as per Manual Section 7)
    fraud_volume = df_dec[df_dec['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    fraud_rate = (fraud_volume / total_volume) if total_volume > 0 else 0.0

    # 4. Calculate Fees per Transaction
    total_fees = 0.0
    
    # Pre-calculate merchant context constants
    m_ctx = {
        'account_type': merchant_profile['account_type'],
        'mcc': merchant_profile['merchant_category_code'],
        'capture_delay': merchant_profile['capture_delay'],
        'monthly_volume': total_volume,
        'monthly_fraud_rate': fraud_rate
    }

    for _, tx in df_dec.iterrows():
        # Build full context for this transaction
        ctx = m_ctx.copy()
        ctx.update({
            'card_scheme': tx['card_scheme'],
            'is_credit': bool(tx['is_credit']),
            'aci': tx['aci'],
            'is_intracountry': (tx['issuing_country'] == tx['acquirer_country'])
        })
        
        # Find the first matching rule
        matched_rule = None
        for rule in fees:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            # Fee Formula: fixed + (rate * amount / 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000.0)
            total_fees += fee
        else:
            # Fallback or error logging if no rule matches (should not happen with complete ruleset)
            pass

    # 5. Output Result
    print(f"{total_fees:.2f}")

if __name__ == "__main__":
    main()

# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2641
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 8354 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

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
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_volume_range(rule_vol, actual_vol):
    """Check if actual volume falls within the rule's volume range string."""
    if rule_vol is None:
        return True
    
    def parse_val(s):
        s = s.lower().strip()
        if 'k' in s: return float(s.replace('k', '')) * 1000
        if 'm' in s: return float(s.replace('m', '')) * 1000000
        return float(s)

    try:
        if '-' in rule_vol:
            low, high = rule_vol.split('-')
            return parse_val(low) <= actual_vol <= parse_val(high)
        elif '>' in rule_vol:
            val = parse_val(rule_vol.replace('>', ''))
            return actual_vol > val
        elif '<' in rule_vol:
            val = parse_val(rule_vol.replace('<', ''))
            return actual_vol < val
    except:
        return False
    return False

def parse_fraud_range(rule_fraud, actual_fraud_rate):
    """Check if actual fraud rate falls within the rule's fraud level string."""
    if rule_fraud is None:
        return True
    
    def parse_pct(s):
        return coerce_to_float(s)

    try:
        if '-' in rule_fraud:
            low, high = rule_fraud.split('-')
            return parse_pct(low) <= actual_fraud_rate <= parse_pct(high)
        elif '>' in rule_fraud:
            val = parse_pct(rule_fraud.replace('>', ''))
            return actual_fraud_rate > val
        elif '<' in rule_fraud:
            val = parse_pct(rule_fraud.replace('<', ''))
            return actual_fraud_rate < val
    except:
        return False
    return False

def check_capture_delay(rule_delay, merchant_delay):
    """Match merchant capture delay to rule requirement."""
    if rule_delay is None:
        return True
    
    # Direct string match
    if str(rule_delay) == str(merchant_delay):
        return True
    
    # Numeric comparison logic
    try:
        if merchant_delay == 'immediate': m_val = 0
        elif merchant_delay == 'manual': m_val = 999
        else: m_val = float(merchant_delay)
    except ValueError:
        return False

    try:
        if '-' in rule_delay:
            low, high = map(float, rule_delay.split('-'))
            return low <= m_val <= high
        elif '>' in rule_delay:
            val = float(rule_delay.replace('>', ''))
            return m_val > val
        elif '<' in rule_delay:
            val = float(rule_delay.replace('<', ''))
            return m_val < val
    except:
        return False
    return False

def match_fee_rule(tx_ctx, rule):
    """Check if a fee rule applies to a transaction context."""
    # 1. Card Scheme
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Merchant Category Code
    if rule['merchant_category_code'] is not None:
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 3. Account Type
    if rule['account_type'] is not None and len(rule['account_type']) > 0:
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 4. Capture Delay
    if not check_capture_delay(rule['capture_delay'], tx_ctx['capture_delay']):
        return False

    # 5. Monthly Volume
    if not parse_volume_range(rule['monthly_volume'], tx_ctx['monthly_volume']):
        return False

    # 6. Monthly Fraud Level
    if not parse_fraud_range(rule['monthly_fraud_level'], tx_ctx['monthly_fraud_level']):
        return False

    # 7. Is Credit
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 8. ACI
    if rule['aci'] is not None and len(rule['aci']) > 0:
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 9. Intracountry
    if rule['intracountry'] is not None:
        if rule['intracountry'] != tx_ctx['intracountry']:
            return False

    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

def main():
    # File paths
    payments_path = '/output/chunk4/data/context/payments.csv'
    fees_path = '/output/chunk4/data/context/fees.json'
    merchant_data_path = '/output/chunk4/data/context/merchant_data.json'

    # 1. Load Data
    try:
        df = pd.read_csv(payments_path)
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        with open(merchant_data_path, 'r') as f:
            merchant_data_list = json.load(f)
    except Exception as e:
        print(f"Error loading files: {e}")
        return

    # 2. Filter for Merchant and Month (July: Day 182-212)
    target_merchant = 'Golfclub_Baron_Friso'
    start_day = 182
    end_day = 212

    df_filtered = df[
        (df['merchant'] == target_merchant) &
        (df['day_of_year'] >= start_day) &
        (df['day_of_year'] <= end_day)
    ].copy()

    if df_filtered.empty:
        print("No transactions found.")
        return

    # 3. Get Merchant Attributes
    merchant_info = next((m for m in merchant_data_list if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found.")
        return

    mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay = merchant_info['capture_delay']

    # 4. Calculate Monthly Aggregates (Volume & Fraud Rate)
    # Manual: Fraud is ratio of fraudulent volume over total volume
    total_volume = df_filtered['eur_amount'].sum()
    fraud_volume = df_filtered[df_filtered['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = fraud_volume / total_volume if total_volume > 0 else 0.0

    # 5. Simulate Fees for Each Scheme
    schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
    scheme_costs = {}

    for scheme in schemes:
        total_fee = 0.0
        
        # Filter rules for this scheme to optimize inner loop
        scheme_rules = [r for r in fees_data if r['card_scheme'] == scheme]
        
        for row in df_filtered.itertuples():
            # Determine Intracountry (Issuer == Acquirer)
            is_intracountry = (row.issuing_country == row.acquirer_country)
            
            # Build Context
            tx_context = {
                'card_scheme': scheme, # Force the scheme being tested
                'merchant_category_code': mcc,
                'account_type': account_type,
                'capture_delay': capture_delay,
                'monthly_volume': total_volume,
                'monthly_fraud_level': fraud_rate,
                'is_credit': row.is_credit,
                'aci': row.aci,
                'intracountry': is_intracountry
            }
            
            # Find Matching Rule
            matched_rule = None
            for rule in scheme_rules:
                if match_fee_rule(tx_context, rule):
                    matched_rule = rule
                    break # Assume first match wins
            
            if matched_rule:
                # Fee = Fixed + (Rate * Amount / 10000)
                fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * row.eur_amount / 10000.0)
                total_fee += fee
            else:
                # If no rule matches, fee is 0 (or could log warning)
                pass

        scheme_costs[scheme] = total_fee

    # 6. Determine Max Fee Scheme
    if not scheme_costs:
        print("No fees calculated.")
        return

    max_scheme = max(scheme_costs, key=scheme_costs.get)
    
    # Output the result
    print(max_scheme)

if __name__ == "__main__":
    main()

# ═══════════════════════════════════════════════════════════
# Round 4 - Task 2759
# ═══════════════════════════════════════════════════════════
# Temperature: 0.35
# Verification: sufficient
# Plan steps: 1
# Code length: 10599 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
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
        if 'k' in v.lower():
            try:
                return float(v.lower().replace('k', '')) * 1000
            except ValueError:
                return 0.0
        if 'm' in v.lower():
            try:
                return float(v.lower().replace('m', '')) * 1000000
            except ValueError:
                return 0.0
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
    
    # Handle > and <
    if s.startswith('>'):
        val = coerce_to_float(s[1:])
        return val, float('inf')
    if s.startswith('<'):
        val = coerce_to_float(s[1:])
        return float('-inf'), val
        
    # Handle ranges with '-'
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            return coerce_to_float(parts[0]), coerce_to_float(parts[1])
            
    # Handle exact match as range [x, x]
    val = coerce_to_float(s)
    return val, val

def check_value_in_range(value, range_str):
    """Checks if a numeric value fits in a range string."""
    if range_str is None: # Wildcard
        return True
    min_val, max_val = parse_range(range_str)
    if min_val is None:
        return True
    
    # Handle exclusive bounds logic based on common business rules
    if range_str.startswith('>'):
        return value > min_val
    if range_str.startswith('<'):
        return value < max_val
        
    return min_val <= value <= max_val

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    """
    # 1. Card Scheme (Explicit match required)
    if rule.get('card_scheme') != tx_ctx['card_scheme']:
        return False
        
    # 2. Monthly Fraud Level (Rule has range string, Tx has float ratio)
    if rule.get('monthly_fraud_level'):
        # Convert rate to percentage for string comparison if needed, 
        # but helper handles float vs range string logic
        if not check_value_in_range(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False

    # 3. Monthly Volume (Rule has range string, Tx has float amount)
    if rule.get('monthly_volume'):
        if not check_value_in_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 4. Is Credit (Rule has bool or null)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 5. ACI (Rule has list, Tx has string)
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Rule has bool/float or null)
    r_intra = rule.get('intracountry')
    if r_intra is not None:
        # In JSON, it might be 0.0/1.0 or boolean
        is_intra_rule = bool(r_intra)
        if is_intra_rule != tx_ctx['intracountry']:
            return False
            
    # 7. Merchant Category Code (Rule has list, Tx has int)
    if rule.get('merchant_category_code'):
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 8. Account Type (Rule has list, Tx has string)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 9. Capture Delay (Rule has string, Tx has string)
    if rule.get('capture_delay'):
        # Direct string match for 'manual', 'immediate'
        if rule['capture_delay'] == tx_ctx['capture_delay']:
            pass
        # Numeric range check if mismatch
        else:
            try:
                val = float(tx_ctx['capture_delay'])
                if not check_value_in_range(val, rule['capture_delay']):
                    return False
            except ValueError:
                return False

    return True

def calculate_fee(amount, rule):
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000)

def get_month(doy):
    """Returns month (1-12) from day of year (1-365)."""
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cumulative = 0
    for i, days in enumerate(days_in_months):
        cumulative += days
        if doy <= cumulative:
            return i + 1
    return 12

# ---------------------------------------------------------
# Main Execution
# ---------------------------------------------------------

def main():
    # 1. Load Data
    try:
        payments = pd.read_csv('/output/chunk4/data/context/payments.csv')
        with open('/output/chunk4/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
        with open('/output/chunk4/data/context/fees.json', 'r') as f:
            fees = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Filter for Target Merchant and Year
    target_merchant = 'Crossfit_Hanna'
    target_year = 2023
    
    df = payments[(payments['merchant'] == target_merchant) & (payments['year'] == target_year)].copy()
    
    if df.empty:
        print("No transactions found for Crossfit_Hanna in 2023.")
        return

    # 3. Get Merchant Metadata
    m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_info:
        print(f"Merchant data for {target_merchant} not found.")
        return

    # 4. Calculate Monthly Stats (Volume and Fraud Rate)
    df['month'] = df['day_of_year'].apply(get_month)
    
    monthly_stats = {}
    for month in df['month'].unique():
        m_df = df[df['month'] == month]
        total_vol = m_df['eur_amount'].sum()
        # Fraud rate is ratio of fraud volume to total volume
        fraud_vol = m_df[m_df['has_fraudulent_dispute']]['eur_amount'].sum()
        fraud_rate = (fraud_vol / total_vol) if total_vol > 0 else 0.0
        
        monthly_stats[month] = {
            'volume': total_vol,
            'fraud_rate': fraud_rate
        }

    # 5. Simulation Loop
    schemes = ['NexPay', 'GlobalCard', 'SwiftCharge', 'TransactPlus']
    scheme_totals = {s: 0.0 for s in schemes}
    
    # Pre-filter rules by merchant static properties to speed up loop
    # We keep rules that match the merchant OR have wildcards for these fields
    candidate_rules = []
    for rule in fees:
        # MCC check
        if rule.get('merchant_category_code') and m_info['merchant_category_code'] not in rule['merchant_category_code']:
            continue
        # Account Type check
        if rule.get('account_type') and m_info['account_type'] not in rule['account_type']:
            continue
        # Capture Delay check
        if rule.get('capture_delay'):
            r_cd = rule['capture_delay']
            m_cd = str(m_info['capture_delay'])
            if r_cd != m_cd:
                # Try numeric check
                try:
                    if not check_value_in_range(float(m_cd), r_cd):
                        continue
                except ValueError:
                    continue
        candidate_rules.append(rule)

    # Iterate through every transaction
    for idx, row in df.iterrows():
        month = row['month']
        stats = monthly_stats.get(month, {'volume': 0, 'fraud_rate': 0})
        
        # Base Context for this transaction (scheme will vary)
        tx_ctx = {
            'monthly_volume': stats['volume'],
            'monthly_fraud_rate': stats['fraud_rate'],
            'is_credit': row['is_credit'],
            'aci': row['aci'],
            'intracountry': row['issuing_country'] == row['acquirer_country'],
            'merchant_category_code': m_info['merchant_category_code'],
            'account_type': m_info['account_type'],
            'capture_delay': str(m_info['capture_delay'])
        }
        
        amount = row['eur_amount']
        
        # Simulate for each scheme
        for scheme in schemes:
            tx_ctx['card_scheme'] = scheme
            
            # Find the first matching rule for this scheme
            matched_rule = None
            for rule in candidate_rules:
                # Optimization: Check scheme first
                if rule.get('card_scheme') != scheme:
                    continue
                
                # Check dynamic transaction properties
                # (Static properties already checked in candidate_rules)
                
                # Monthly Fraud
                if rule.get('monthly_fraud_level') and not check_value_in_range(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
                    continue
                # Monthly Volume
                if rule.get('monthly_volume') and not check_value_in_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
                    continue
                # Is Credit
                if rule.get('is_credit') is not None and rule['is_credit'] != tx_ctx['is_credit']:
                    continue
                # ACI
                if rule.get('aci') and tx_ctx['aci'] not in rule['aci']:
                    continue
                # Intracountry
                if rule.get('intracountry') is not None and bool(rule['intracountry']) != tx_ctx['intracountry']:
                    continue
                
                matched_rule = rule
                break # Stop at first match
            
            if matched_rule:
                fee = calculate_fee(amount, matched_rule)
                scheme_totals[scheme] += fee

    # 6. Result
    # Identify scheme with maximum fees
    max_scheme = max(scheme_totals, key=scheme_totals.get)
    
    # Print result
    print(max_scheme)

if __name__ == "__main__":
    main()

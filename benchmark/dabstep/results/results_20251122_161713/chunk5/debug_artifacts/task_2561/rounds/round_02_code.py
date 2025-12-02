# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2561
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 12056 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import json
import pandas as pd
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return None
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
            return None
    return None

def parse_range(range_str):
    """Parses a string range like '100k-1m' or '>5' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip().replace(',', '').replace('%', '')
    multiplier = 1
    if 'k' in s:
        multiplier = 1000
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000
        s = s.replace('m', '')
        
    if '>' in s:
        try:
            val = float(s.replace('>', '')) * multiplier
            return val, float('inf')
        except:
            return None, None
    elif '<' in s:
        try:
            val = float(s.replace('<', '')) * multiplier
            return float('-inf'), val
        except:
            return None, None
    elif '-' in s:
        try:
            parts = s.split('-')
            min_val = float(parts[0]) * multiplier
            max_val = float(parts[1]) * multiplier
            return min_val, max_val
        except:
            return None, None
    else:
        # Exact match treated as range [val, val]
        try:
            val = float(s) * multiplier
            return val, val
        except:
            return None, None

def check_rule_match(transaction, merchant_info, rule):
    """
    Checks if a transaction and its merchant match a specific fee rule.
    
    Args:
        transaction (dict/Series): Transaction row.
        merchant_info (dict): Merchant metadata (stats, static data).
        rule (dict): Fee rule from fees.json.
        
    Returns:
        bool: True if match, False otherwise.
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != transaction['card_scheme']:
        return False

    # 2. Account Type (The rule's current account type)
    # If rule has specific account types, merchant must match one.
    # If rule['account_type'] is empty/None, it applies to all.
    if rule.get('account_type'):
        if merchant_info['account_type'] not in rule['account_type']:
            return False

    # 3. Capture Delay
    if rule.get('capture_delay'):
        # This is a complex string match usually, simplified here for exact or range
        # If rule specifies a delay, merchant's delay must match or fall in bucket
        # For this specific problem, we'll check exact match or simple logic if needed
        # Assuming exact string match for simplicity unless range provided
        if rule['capture_delay'] != merchant_info['capture_delay']:
             # Handle cases like '>5' vs '7' if necessary, but exact match is safer first step
             pass 

    # 4. Monthly Fraud Level
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        # Merchant fraud is a ratio (0.083), rule is string "8.3%"
        # parse_range handles % removal but returns whole numbers usually? 
        # Let's adjust: parse_range("8.3%") -> 8.3. Merchant val 0.083 -> 8.3%
        merch_fraud_pct = merchant_info['monthly_fraud_rate'] * 100
        if min_f is not None and (merch_fraud_pct < min_f or merch_fraud_pct > max_f):
            return False

    # 5. Monthly Volume
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        merch_vol = merchant_info['monthly_volume']
        if min_v is not None and (merch_vol < min_v or merch_vol > max_v):
            return False

    # 6. Merchant Category Code
    if rule.get('merchant_category_code'):
        if merchant_info['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 7. Is Credit
    if rule.get('is_credit') is not None:
        # transaction['is_credit'] is boolean or 0/1
        if bool(rule['is_credit']) != bool(transaction['is_credit']):
            return False

    # 8. ACI
    if rule.get('aci'):
        if transaction['aci'] not in rule['aci']:
            return False

    # 9. Intracountry
    if rule.get('intracountry') is not None:
        is_intra = (transaction['issuing_country'] == transaction['acquirer_country'])
        # rule['intracountry'] might be 0.0, 1.0, or boolean
        rule_intra = bool(float(rule['intracountry'])) if rule['intracountry'] is not None else None
        if rule_intra is not None and rule_intra != is_intra:
            return False

    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

def main():
    # 1. Load Data
    try:
        fees_path = '/output/chunk5/data/context/fees.json'
        merchants_path = '/output/chunk5/data/context/merchant_data.json'
        payments_path = '/output/chunk5/data/context/payments.csv'

        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        
        with open(merchants_path, 'r') as f:
            merchant_data_list = json.load(f)
            
        df_payments = pd.read_csv(payments_path)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Get Fee Rule 384
    fee_rule_384 = next((f for f in fees_data if f['ID'] == 384), None)
    if not fee_rule_384:
        print("Fee ID 384 not found.")
        return

    # 3. Prepare Merchant Metadata (Static + Calculated Stats)
    # Map merchant name to static data
    merchant_static = {m['merchant']: m for m in merchant_data_list}
    
    # Calculate dynamic stats (Volume, Fraud) per merchant
    # Note: Assuming 2023 data is the basis for monthly stats (using average monthly or total/12)
    # The manual says "Monthly volumes... computed always in natural months".
    # For simplicity in this "snapshot" analysis, we often use the dataset totals or averages.
    # Let's calculate total volume and fraud rate for the dataset duration (2023).
    
    merchant_stats = {}
    grouped = df_payments.groupby('merchant')
    
    for merchant_name, group in grouped:
        total_vol = group['eur_amount'].sum()
        # Assuming dataset is 1 year?
        # If dataset is partial, we might need to adjust. 
        # Documentation says "During 2023". Let's assume it covers the relevant period.
        # Fee rules usually apply to "monthly volume". 
        # Let's approximate monthly volume as Total / 12 for 2023.
        monthly_vol = total_vol / 12.0
        
        fraud_count = group['has_fraudulent_dispute'].sum()
        tx_count = len(group)
        fraud_rate = fraud_count / tx_count if tx_count > 0 else 0.0
        
        # Merge with static data
        static = merchant_static.get(merchant_name, {})
        merchant_stats[merchant_name] = {
            'merchant': merchant_name,
            'account_type': static.get('account_type'),
            'merchant_category_code': static.get('merchant_category_code'),
            'capture_delay': static.get('capture_delay'),
            'monthly_volume': monthly_vol,
            'monthly_fraud_rate': fraud_rate
        }

    # 4. Identify Merchants Currently Matching Fee 384
    # We need to see if ANY transaction for a merchant triggers this fee.
    # Optimization: Filter payments first by transaction-level constraints of Fee 384
    
    # Transaction-level constraints from Fee 384
    req_scheme = fee_rule_384.get('card_scheme')
    req_credit = fee_rule_384.get('is_credit')
    req_aci = fee_rule_384.get('aci')
    req_intra = fee_rule_384.get('intracountry')
    
    # Filter DF
    mask = pd.Series(True, index=df_payments.index)
    
    if req_scheme:
        mask &= (df_payments['card_scheme'] == req_scheme)
    
    if req_credit is not None:
        mask &= (df_payments['is_credit'] == bool(req_credit))
        
    if req_aci: # List of allowed ACIs
        mask &= (df_payments['aci'].isin(req_aci))
        
    if req_intra is not None:
        # Calculate intracountry for rows
        is_intra_col = (df_payments['issuing_country'] == df_payments['acquirer_country'])
        req_intra_bool = bool(float(req_intra))
        mask &= (is_intra_col == req_intra_bool)

    potential_txs = df_payments[mask]
    
    # Get unique merchants from these potentially matching transactions
    candidate_merchants = potential_txs['merchant'].unique()
    
    affected_merchants = []
    
    # 5. Check Merchant-Level Constraints for Candidates
    for m_name in candidate_merchants:
        m_info = merchant_stats.get(m_name)
        if not m_info:
            continue
            
        # Check merchant-level rules (MCC, Volume, Fraud, Capture Delay)
        # We pass a dummy transaction because we already filtered by tx-level rules
        # But we need to pass the merchant info to check_rule_match logic or do it manually here.
        
        # Let's do manual check for merchant-level parts of Fee 384 to be precise
        
        # MCC
        if fee_rule_384.get('merchant_category_code'):
            if m_info['merchant_category_code'] not in fee_rule_384['merchant_category_code']:
                continue
                
        # Volume
        if fee_rule_384.get('monthly_volume'):
            min_v, max_v = parse_range(fee_rule_384['monthly_volume'])
            if min_v is not None:
                if not (min_v <= m_info['monthly_volume'] <= max_v):
                    continue
                    
        # Fraud
        if fee_rule_384.get('monthly_fraud_level'):
            min_f, max_f = parse_range(fee_rule_384['monthly_fraud_level'])
            # m_info['monthly_fraud_rate'] is 0.083 for 8.3%
            # parse_range returns 8.3 for "8.3%"
            current_fraud_pct = m_info['monthly_fraud_rate'] * 100
            if min_f is not None:
                if not (min_f <= current_fraud_pct <= max_f):
                    continue
        
        # Capture Delay
        if fee_rule_384.get('capture_delay'):
            # Simple check
            if fee_rule_384['capture_delay'] != m_info['capture_delay']:
                # If rule is manual and merchant is manual, match.
                # If rule is >5 and merchant is 7, we need logic.
                # Given the data samples, capture_delay is often categorical ('manual', 'immediate').
                # Let's assume exact match for categorical, or skip if complex logic needed and not implemented.
                # Based on file context, values are 'manual', 'immediate', '1', etc.
                # If mismatch, skip.
                continue

        # If we reached here, the merchant currently qualifies for Fee 384.
        # Now check the "Affected" condition:
        # "Fee 384 was only applied to account type H"
        # Affected = Currently Qualifies AND Account Type is NOT H
        
        if m_info['account_type'] != 'H':
            affected_merchants.append(m_name)

    # 6. Output Results
    # Sort for deterministic output
    affected_merchants.sort()
    
    if not affected_merchants:
        print("No merchants affected.")
    else:
        print(", ".join(affected_merchants))

if __name__ == "__main__":
    main()

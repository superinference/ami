# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1740
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 11378 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float. Handles common formats."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        
        # Handle percentages
        if '%' in v:
            return float(v.replace('%', '')) / 100
            
        # Handle k/m suffixes
        multiplier = 1
        if v.endswith('k'):
            multiplier = 1_000
            v = v[:-1]
        elif v.endswith('m'):
            multiplier = 1_000_000
            v = v[:-1]
            
        # Handle ranges (e.g., "100k-1m") - return mean for single value context, 
        # but for range checking we usually parse differently. 
        # This function is for simple coercion.
        if '-' in v:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2 * multiplier
            except:
                pass
                
        try:
            return float(v) * multiplier
        except ValueError:
            return None
    return None

def parse_range(range_str):
    """Parses a string range like '100k-1m', '>5', '<3' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.strip().lower().replace(',', '').replace('%', '')
    is_percent = '%' in range_str
    
    # Helper to parse number with k/m
    def parse_num(n_str):
        m = 1
        if n_str.endswith('k'):
            m = 1000
            n_str = n_str[:-1]
        elif n_str.endswith('m'):
            m = 1000000
            n_str = n_str[:-1]
        val = float(n_str)
        return val / 100 if is_percent else val

    try:
        if '-' in s:
            parts = s.split('-')
            return parse_num(parts[0]), parse_num(parts[1])
        elif s.startswith('>'):
            return parse_num(s[1:]), float('inf')
        elif s.startswith('<'):
            return float('-inf'), parse_num(s[1:])
        else:
            val = parse_num(s)
            return val, val # Exact match treated as range [val, val]
    except:
        return None, None

def check_range_match(value, rule_value):
    """Checks if a numeric value fits within a rule's range string."""
    if rule_value is None:
        return True # Wildcard matches all
    
    min_val, max_val = parse_range(rule_value)
    if min_val is None:
        return True # Failed to parse, assume match or ignore? Assume match to be safe/wildcard
        
    return min_val <= value <= max_val

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_context must contain:
    - card_scheme (str)
    - is_credit (bool)
    - aci (str)
    - intracountry (bool)
    - account_type (str)
    - merchant_category_code (int)
    - capture_delay (str)
    - monthly_volume (float)
    - monthly_fraud_level (float)
    """
    
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match or Empty/Null wildcard)
    # Rule has list of types. Merchant has one type.
    if rule.get('account_type'): # If rule has specific types
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match or Empty/Null wildcard)
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Exact match or Null wildcard)
    if rule.get('capture_delay') and rule['capture_delay'] != tx_context['capture_delay']:
        # Note: capture_delay in rules can be ranges like '3-5', but merchant data is usually specific 'manual', 'immediate'.
        # If rule is a range, we might need range logic, but usually capture_delay is categorical in this dataset.
        # Let's check if rule is a range string vs exact string.
        # Based on file analysis, merchant has 'manual', 'immediate', '1'. Rules have 'manual', 'immediate', '>5'.
        # Simple string match is safer unless we see numeric delays.
        # If rule is '>5' and merchant is '7', that's a match.
        # Let's try simple match first, then range if needed.
        r_delay = str(rule['capture_delay'])
        m_delay = str(tx_context['capture_delay'])
        
        if r_delay == m_delay:
            pass # Match
        elif r_delay.startswith('>') or r_delay.startswith('<') or '-' in r_delay:
            # It's a range rule. Try to parse merchant delay as int.
            try:
                delay_days = float(m_delay)
                if not check_range_match(delay_days, r_delay):
                    return False
            except ValueError:
                # Merchant delay is 'manual' or 'immediate', rule is numeric range -> No match
                return False
        else:
            return False

    # 5. Monthly Volume (Range match or Null wildcard)
    if rule.get('monthly_volume'):
        if not check_range_match(tx_context['monthly_volume'], rule['monthly_volume']):
            return False
            
    # 6. Monthly Fraud Level (Range match or Null wildcard)
    if rule.get('monthly_fraud_level'):
        if not check_range_match(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    # 7. Is Credit (Bool match or Null wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 8. ACI (List match or Null wildcard)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry (Bool match or Null wildcard)
    if rule.get('intracountry') is not None:
        if rule['intracountry'] != tx_context['intracountry']:
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0)
    rate = rule.get('rate', 0)
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000)

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

def main():
    # File paths
    payments_path = '/output/chunk4/data/context/payments.csv'
    fees_path = '/output/chunk4/data/context/fees.json'
    merchant_path = '/output/chunk4/data/context/merchant_data.json'
    
    print("Loading data...")
    try:
        df_payments = pd.read_csv(payments_path)
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        with open(merchant_path, 'r') as f:
            merchant_data = json.load(f)
    except Exception as e:
        print(f"Error loading files: {e}")
        return

    target_merchant = 'Rafa_AI'
    target_year = 2023
    target_day = 365
    
    # 1. Get Merchant Metadata
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return
        
    print(f"Merchant Info: {merchant_info}")
    
    # 2. Calculate Monthly Stats for December 2023 (Days 335-365)
    # December starts on day 335 (non-leap year).
    dec_start = 335
    dec_end = 365
    
    print(f"Calculating stats for Dec 2023 (Day {dec_start}-{dec_end})...")
    
    dec_txs = df_payments[
        (df_payments['merchant'] == target_merchant) &
        (df_payments['year'] == target_year) &
        (df_payments['day_of_year'] >= dec_start) &
        (df_payments['day_of_year'] <= dec_end)
    ]
    
    monthly_volume = dec_txs['eur_amount'].sum()
    monthly_fraud_count = dec_txs['has_fraudulent_dispute'].sum()
    monthly_tx_count = len(dec_txs)
    
    monthly_fraud_rate = 0.0
    if monthly_volume > 0: # Fraud level is usually ratio of volume or count?
        # Manual says: "fraud levels measured as ratio between monthly total volume and monthly volume notified as fraud"
        # Wait, manual says: "ratio between monthly total volume and monthly volume notified as fraud"
        # Actually, usually it's (Fraud Volume / Total Volume).
        # Let's check manual text carefully: "ratio between monthly total volume and monthly volume notified as fraud"
        # This phrasing is slightly ambiguous ("between X and Y"). Usually means Y/X.
        # Let's calculate Fraud Volume.
        fraud_volume = dec_txs[dec_txs['has_fraudulent_dispute']]['eur_amount'].sum()
        monthly_fraud_rate = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0
    
    print(f"Monthly Volume: €{monthly_volume:,.2f}")
    print(f"Monthly Fraud Volume: €{fraud_volume:,.2f}")
    print(f"Monthly Fraud Rate: {monthly_fraud_rate:.4%}")
    
    # 3. Filter Target Transactions (Day 365)
    target_txs = df_payments[
        (df_payments['merchant'] == target_merchant) &
        (df_payments['year'] == target_year) &
        (df_payments['day_of_year'] == target_day)
    ]
    
    print(f"Found {len(target_txs)} transactions for {target_merchant} on Day {target_day}.")
    
    # 4. Calculate Fees
    total_fees = 0.0
    
    # Sort fees by ID to ensure consistent priority (assuming lower ID = higher priority, or just list order)
    # The problem doesn't specify priority, but usually JSON list order matters.
    # We'll iterate through the list as provided.
    
    for idx, tx in target_txs.iterrows():
        # Build context for matching
        is_intra = (tx['issuing_country'] == tx['acquirer_country'])
        
        context = {
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': is_intra,
            'account_type': merchant_info['account_type'],
            'merchant_category_code': merchant_info['merchant_category_code'],
            'capture_delay': merchant_info['capture_delay'],
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_rate
        }
        
        # Find first matching rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(context, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fees += fee
            # print(f"Tx {tx['psp_reference']}: Amount {tx['eur_amount']} -> Fee {fee:.4f} (Rule ID {matched_rule['ID']})")
        else:
            print(f"WARNING: No fee rule matched for Tx {tx['psp_reference']}")
            
    print(f"\nTotal Fees for Rafa_AI on Day 365: €{total_fees:.14f}")
    print(total_fees) # Final answer format

if __name__ == "__main__":
    main()

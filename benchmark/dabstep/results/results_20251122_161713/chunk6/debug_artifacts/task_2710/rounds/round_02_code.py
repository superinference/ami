# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2710
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9525 characters (FULL CODE)
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
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        return float(v)
    return float(value)

def parse_range(range_str):
    """Parses a range string (e.g., '100k-1m', '>5', '7.7%-8.3%') into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle k/m suffixes
    def parse_val(x):
        if 'k' in x: return float(x.replace('k', '')) * 1000
        if 'm' in x: return float(x.replace('m', '')) * 1000000
        if '%' in x: return float(x.replace('%', '')) / 100
        return float(x)

    try:
        if '-' in s:
            parts = s.split('-')
            return parse_val(parts[0]), parse_val(parts[1])
        elif s.startswith('>'):
            return parse_val(s[1:]), float('inf')
        elif s.startswith('<'):
            return float('-inf'), parse_val(s[1:])
        elif s == 'immediate':
            return 0, 0 # Treat as numeric 0 for comparison if needed, or handle as string match
        elif s == 'manual':
            return 999, 999
    except:
        pass
    return None, None

def check_range_match(value, rule_range_str):
    """Checks if a numeric value falls within a rule's range string."""
    if rule_range_str is None:
        return True
    
    # Special string handling for capture_delay
    if isinstance(value, str) and isinstance(rule_range_str, str):
        # Exact string match for things like 'immediate', 'manual'
        if rule_range_str == value:
            return True
        # If rule is range like '3-5' and value is numeric string '4', handle below
        try:
            val_float = float(value)
        except:
            return False # Value is string (e.g. 'manual') but rule is range (e.g. '>5') -> No match
            
    # Numeric handling
    try:
        val_float = float(value)
    except:
        return False

    min_v, max_v = parse_range(rule_range_str)
    if min_v is None: 
        # Fallback for non-standard strings that parse_range didn't catch
        return False
        
    return min_v <= val_float <= max_v

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_context must contain:
      - card_scheme (str)
      - account_type (str)
      - mcc (int)
      - is_credit (bool)
      - aci (str)
      - capture_delay (str)
      - intracountry (bool)
      - monthly_volume (float)
      - monthly_fraud_level (float)
    """
    # 1. Card Scheme
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (Rule has list, tx has single value)
    if rule['account_type'] and tx_context['account_type'] not in rule['account_type']:
        return False

    # 3. MCC (Rule has list, tx has single value)
    if rule['merchant_category_code'] and tx_context['mcc'] not in rule['merchant_category_code']:
        return False

    # 4. Is Credit (Rule has bool or None)
    if rule['is_credit'] is not None and rule['is_credit'] != tx_context['is_credit']:
        return False

    # 5. ACI (Rule has list, tx has single value)
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False

    # 6. Intracountry (Rule has 1.0/0.0/None)
    if rule['intracountry'] is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    # 7. Capture Delay (Rule has string range or exact string, tx has string)
    # If rule is null, matches any. If rule is set, must match.
    if rule['capture_delay'] is not None:
        # Direct match or range match
        if rule['capture_delay'] == tx_context['capture_delay']:
            pass # Match
        elif not check_range_match(tx_context['capture_delay'], rule['capture_delay']):
            return False

    # 8. Monthly Volume
    if rule['monthly_volume'] is not None:
        if not check_range_match(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level
    if rule['monthly_fraud_level'] is not None:
        if not check_range_match(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    # fee = fixed + (rate * amount / 10000)
    # Rate is in basis points per 100 (e.g. 19 means 0.19%? No, manual says "divided by 10000")
    # Manual: "rate: integer... multiplied by the transaction value and divided by 10000"
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000.0)

# ---------------------------------------------------------
# MAIN ANALYSIS
# ---------------------------------------------------------

def main():
    # File paths
    payments_path = '/output/chunk6/data/context/payments.csv'
    merchant_path = '/output/chunk6/data/context/merchant_data.json'
    fees_path = '/output/chunk6/data/context/fees.json'

    # Load data
    df_payments = pd.read_csv(payments_path)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)

    # Target Merchant and Timeframe
    target_merchant = 'Martinis_Fine_Steakhouse'
    start_day = 60
    end_day = 90
    
    # 1. Get Merchant Metadata
    m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_info:
        print("Merchant not found in merchant_data.json")
        return

    account_type = m_info['account_type']
    mcc = m_info['merchant_category_code']
    capture_delay = m_info['capture_delay']

    # 2. Calculate March Stats (Volume & Fraud Rate)
    # Filter for March transactions for this merchant
    march_txs = df_payments[
        (df_payments['merchant'] == target_merchant) &
        (df_payments['day_of_year'] >= start_day) &
        (df_payments['day_of_year'] <= end_day)
    ]

    total_volume = march_txs['eur_amount'].sum()
    fraud_volume = march_txs[march_txs['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    
    # Avoid division by zero
    fraud_rate = (fraud_volume / total_volume) if total_volume > 0 else 0.0

    print(f"Merchant: {target_merchant}")
    print(f"March Volume: €{total_volume:,.2f}")
    print(f"March Fraud Rate: {fraud_rate:.4%}")
    print(f"Account Type: {account_type}, MCC: {mcc}, Capture Delay: {capture_delay}")

    # 3. Identify Target Transactions (Fraudulent ones in March)
    target_txs = march_txs[march_txs['has_fraudulent_dispute'] == True].copy()
    print(f"Number of fraudulent transactions to simulate: {len(target_txs)}")

    if len(target_txs) == 0:
        print("No fraudulent transactions found to analyze.")
        return

    # 4. Simulate ACIs
    possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    results = {}

    for sim_aci in possible_acis:
        total_fee_aci = 0.0
        
        for _, tx in target_txs.iterrows():
            # Determine intracountry
            is_intra = (tx['issuing_country'] == tx['acquirer_country'])
            
            # Build context
            context = {
                'card_scheme': tx['card_scheme'],
                'account_type': account_type,
                'mcc': mcc,
                'is_credit': tx['is_credit'],
                'aci': sim_aci, # <--- The variable we are changing
                'capture_delay': capture_delay,
                'intracountry': is_intra,
                'monthly_volume': total_volume,
                'monthly_fraud_level': fraud_rate
            }
            
            # Find matching rule
            matched_rule = None
            # Iterate through fees to find match. 
            # Assuming fees.json order matters or first match is sufficient.
            # In typical fee structures, specific rules override general ones, 
            # but without explicit priority logic, first match is standard.
            for rule in fees_data:
                if match_fee_rule(context, rule):
                    matched_rule = rule
                    break
            
            if matched_rule:
                fee = calculate_fee(tx['eur_amount'], matched_rule)
                total_fee_aci += fee
            else:
                # Fallback if no rule matches (should not happen in synthetic data)
                # print(f"Warning: No rule found for tx {tx['psp_reference']} with ACI {sim_aci}")
                pass

        results[sim_aci] = total_fee_aci

    # 5. Find Best ACI
    print("\nSimulation Results (Total Fees for Fraudulent Txs):")
    for aci, fee in results.items():
        print(f"ACI {aci}: €{fee:.2f}")

    best_aci = min(results, key=results.get)
    print(f"\nPreferred ACI with lowest fees: {best_aci}")
    
    # Output just the answer as requested by the prompt style usually
    print(best_aci)

if __name__ == "__main__":
    main()

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2767
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 6698 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- HELPER FUNCTIONS ---
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
        return float(v)
    return float(value) if value is not None else 0.0

def parse_value(val_str):
    """Helper to parse individual values with units like k, m, %."""
    if not isinstance(val_str, str): return 0.0
    s = val_str.strip().lower().replace(',', '')
    
    # Handle percentage
    is_percent = '%' in s
    s = s.replace('%', '')
    
    # Handle units
    factor = 1.0
    if 'k' in s: 
        factor = 1000.0
        s = s.replace('k', '')
    elif 'm' in s: 
        factor = 1000000.0
        s = s.replace('m', '')
        
    scale = 0.01 if is_percent else 1.0
    
    try:
        return float(s) * factor * scale
    except:
        return 0.0

def parse_range(range_str):
    """Parses range strings into (min, max)."""
    if not isinstance(range_str, str): return None, None
    s = range_str.strip().lower()
    
    if '-' in s:
        parts = s.split('-')
        return parse_value(parts[0]), parse_value(parts[1])
    elif '>' in s:
        return parse_value(s.replace('>', '')), float('inf')
    elif '<' in s:
        return float('-inf'), parse_value(s.replace('<', ''))
    else:
        v = parse_value(s)
        return v, v

def check_range(value, range_str):
    """Checks if value is in range_str."""
    if range_str is None: return True
    low, high = parse_range(range_str)
    if low is None: return True
    return low <= value <= high

def match_fee_rule(tx_ctx, rule):
    """Matches transaction context to fee rule."""
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False
    # 2. Account Type (List)
    if rule.get('account_type') and tx_ctx.get('account_type') not in rule['account_type']:
        return False
    # 3. MCC (List)
    if rule.get('merchant_category_code') and tx_ctx.get('mcc') not in rule['merchant_category_code']:
        return False
    # 4. Capture Delay
    if rule.get('capture_delay') and rule['capture_delay'] != tx_ctx.get('capture_delay'):
        return False
    # 5. Is Credit
    if rule.get('is_credit') is not None and rule['is_credit'] != tx_ctx.get('is_credit'):
        return False
    # 6. ACI (List)
    if rule.get('aci') and tx_ctx.get('aci') not in rule['aci']:
        return False
    # 7. Intracountry
    if rule.get('intracountry') is not None and rule['intracountry'] != tx_ctx.get('intracountry'):
        return False
    # 8. Monthly Volume
    if rule.get('monthly_volume') and not check_range(tx_ctx.get('monthly_volume'), rule['monthly_volume']):
        return False
    # 9. Monthly Fraud Level
    if rule.get('monthly_fraud_level') and not check_range(tx_ctx.get('monthly_fraud_level'), rule['monthly_fraud_level']):
        return False
        
    return True

def calculate_fee(amount, rule):
    """Calculates fee: fixed + (rate * amount / 10000)."""
    fixed = float(rule.get('fixed_amount', 0.0) or 0.0)
    rate = float(rule.get('rate', 0.0) or 0.0)
    return fixed + (rate * amount / 10000.0)

def execute_step():
    # Load Data
    payments_path = '/output/chunk2/data/context/payments.csv'
    fees_path = '/output/chunk2/data/context/fees.json'
    merchant_path = '/output/chunk2/data/context/merchant_data.json'
    
    df = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees = json.load(f)
    with open(merchant_path, 'r') as f:
        merchants = json.load(f)
        
    target_merchant = 'Belles_cookbook_store'
    
    # Get Merchant Info
    merchant_info = next((m for m in merchants if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print("Merchant not found")
        return

    # Calculate 2023 Stats for Fee Tier
    # Filter for merchant and year 2023
    df_merchant_2023 = df[(df['merchant'] == target_merchant) & (df['year'] == 2023)]
    
    total_vol = df_merchant_2023['eur_amount'].sum()
    monthly_vol = total_vol / 12.0
    
    fraud_vol = df_merchant_2023[df_merchant_2023['has_fraudulent_dispute'] == True]['eur_amount'].sum()
    monthly_fraud_pct = fraud_vol / total_vol if total_vol > 0 else 0.0
    
    print(f"Merchant Stats 2023: Vol={monthly_vol:.2f}, Fraud={monthly_fraud_pct:.4%}")
    print(f"Account Type: {merchant_info['account_type']}")
    print(f"MCC: {merchant_info['merchant_category_code']}")
    
    # Identify Fraudulent Transactions to Optimize
    target_txs = df_merchant_2023[df_merchant_2023['has_fraudulent_dispute'] == True].copy()
    print(f"Simulating fees for {len(target_txs)} fraudulent transactions...")
    
    # Simulate ACIs
    possible_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    results = {}
    
    for aci in possible_acis:
        total_fee = 0.0
        
        for _, row in target_txs.iterrows():
            ctx = {
                'card_scheme': row['card_scheme'],
                'account_type': merchant_info['account_type'],
                'mcc': merchant_info['merchant_category_code'],
                'capture_delay': merchant_info['capture_delay'],
                'is_credit': row['is_credit'],
                'aci': aci, # The variable we are optimizing
                'intracountry': row['issuing_country'] == row['acquirer_country'],
                'monthly_volume': monthly_vol,
                'monthly_fraud_level': monthly_fraud_pct
            }
            
            # Find first matching rule
            matched = False
            for rule in fees:
                if match_fee_rule(ctx, rule):
                    total_fee += calculate_fee(row['eur_amount'], rule)
                    matched = True
                    break
            
            if not matched:
                # Should not happen with complete fee rules, but good to know
                pass
                
        results[aci] = total_fee
        print(f"ACI {aci}: €{total_fee:.2f}")
        
    # Find Best
    best_aci = min(results, key=results.get)
    print(f"\nLowest Fee ACI: {best_aci}")
    print(best_aci)

if __name__ == "__main__":
    execute_step()

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1823
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8825 characters (FULL CODE)
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
            return 0.0
    return float(value)

def parse_range(range_str):
    """Parses a string range like '100k-1m' or '>5' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle k/m suffixes
    def parse_val(val_s):
        val_s = val_s.strip()
        multiplier = 1
        if val_s.endswith('%'):
            val_s = val_s[:-1]
            multiplier = 0.01
        elif val_s.endswith('k'):
            val_s = val_s[:-1]
            multiplier = 1000
        elif val_s.endswith('m'):
            val_s = val_s[:-1]
            multiplier = 1000000
        return float(val_s) * multiplier

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('>'):
        return parse_val(s[1:]), float('inf')
    elif s.startswith('<'):
        return float('-inf'), parse_val(s[1:])
    else:
        # Exact match treated as range [val, val]
        try:
            val = parse_val(s)
            return val, val
        except:
            return None, None

def check_range(value, range_str):
    """Checks if a value falls within a string range."""
    if range_str is None:
        return True
    min_val, max_val = parse_range(range_str)
    if min_val is None: 
        # If parsing failed, assume it's a categorical string match (e.g. "manual")
        return str(value).lower() == str(range_str).lower()
    return min_val <= value <= max_val

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_context must contain:
    - card_scheme, account_type, mcc, is_credit, aci, intracountry, 
    - capture_delay, monthly_volume, monthly_fraud_level
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match)
    # Rule has list of types. Merchant has single type.
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Convert rule value to bool if it's 0.0/1.0
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    # 7. Capture Delay (String/Range match)
    # Merchant has specific delay (e.g. "manual"). Rule might be "manual" or range.
    if rule.get('capture_delay'):
        # Direct string match first
        if rule['capture_delay'] == tx_context['capture_delay']:
            pass
        # Check if it's a range (unlikely for 'manual', but possible for days)
        elif not check_range(tx_context['capture_delay'], rule['capture_delay']):
             return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range(tx_context['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_range(tx_context['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
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
    print("Loading data...")
    df = pd.read_csv(payments_path)
    
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
        
    with open(fees_path, 'r') as f:
        fees = json.load(f)
        
    # 2. Filter for Crossfit_Hanna, Jan 2023
    target_merchant = 'Crossfit_Hanna'
    # Jan 2023: year=2023, day_of_year <= 31
    df_jan = df[
        (df['merchant'] == target_merchant) & 
        (df['year'] == 2023) & 
        (df['day_of_year'] <= 31)
    ].copy()
    
    print(f"Found {len(df_jan)} transactions for {target_merchant} in Jan 2023.")
    
    # 3. Get Merchant Metadata
    # Find merchant config
    m_config = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_config:
        print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
        return

    account_type = m_config['account_type']
    mcc = m_config['merchant_category_code']
    capture_delay = m_config['capture_delay']
    
    # 4. Calculate Monthly Stats (Volume & Fraud)
    # Volume in Euros
    monthly_volume = df_jan['eur_amount'].sum()
    
    # Fraud Volume (sum of amounts where has_fraudulent_dispute is True)
    fraud_volume = df_jan[df_jan['has_fraudulent_dispute']]['eur_amount'].sum()
    
    # Fraud Rate (Fraud Volume / Total Volume)
    if monthly_volume > 0:
        monthly_fraud_level = fraud_volume / monthly_volume
    else:
        monthly_fraud_level = 0.0
        
    print(f"Monthly Volume: €{monthly_volume:,.2f}")
    print(f"Monthly Fraud Volume: €{fraud_volume:,.2f}")
    print(f"Monthly Fraud Rate: {monthly_fraud_level:.4%}")
    
    # 5. Calculate Fees per Transaction
    total_fees = 0.0
    
    # Pre-calculate context parts that don't change per transaction
    base_context = {
        'account_type': account_type,
        'mcc': mcc,
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    # Sort fees by ID to ensure deterministic matching order (though usually first match in list is standard)
    # Assuming fees.json order is priority order.
    
    match_count = 0
    
    for _, tx in df_jan.iterrows():
        # Build transaction specific context
        tx_context = base_context.copy()
        tx_context['card_scheme'] = tx['card_scheme']
        tx_context['is_credit'] = tx['is_credit']
        tx_context['aci'] = tx['aci']
        tx_context['intracountry'] = (tx['issuing_country'] == tx['acquirer_country'])
        
        # Find matching rule
        matched_rule = None
        for rule in fees:
            if match_fee_rule(tx_context, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fees += fee
            match_count += 1
        else:
            # Fallback or error if no rule matches? 
            # In real scenarios, there's usually a default. Here we assume coverage.
            # print(f"No rule matched for tx {tx['psp_reference']}")
            pass
            
    print(f"Matched rules for {match_count}/{len(df_jan)} transactions.")
    print(f"Total Fees: {total_fees}")
    
    # Output just the number as requested by "What are the total fees"
    # Usually we print the value cleanly.
    print(f"{total_fees:.14f}")

if __name__ == "__main__":
    main()

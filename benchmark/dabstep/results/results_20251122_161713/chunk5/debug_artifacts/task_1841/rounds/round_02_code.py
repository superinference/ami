# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1841
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7783 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
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
        except:
            pass
    return float(value) if value is not None else 0.0

def parse_value(val_str):
    """Parses a single value string with units (k, m, %) into a float."""
    v = val_str.lower().strip()
    scale = 1.0
    if '%' in v:
        scale = 0.01
        v = v.replace('%', '')
    if 'k' in v:
        scale *= 1000.0
        v = v.replace('k', '')
    if 'm' in v:
        scale *= 1000000.0
        v = v.replace('m', '')
    return float(v) * scale

def parse_range_check(rule_val, actual_val):
    """
    Parses rule_val (e.g. '100k-1m', '>8.3%', '3-5') and checks if actual_val fits.
    Returns True if rule_val is None (wildcard) or matches.
    """
    if rule_val is None:
        return True
    
    if isinstance(rule_val, str):
        # Exact string matches (e.g. 'manual', 'immediate')
        if rule_val in ['manual', 'immediate']:
            return str(actual_val) == rule_val
            
        rv = rule_val.lower()
        try:
            # Range "min-max"
            if '-' in rv:
                low_s, high_s = rv.split('-')
                return parse_value(low_s) <= actual_val <= parse_value(high_s)
            
            # Inequality ">val"
            if rv.startswith('>'):
                return actual_val > parse_value(rv[1:])
                
            # Inequality "<val"
            if rv.startswith('<'):
                return actual_val < parse_value(rv[1:])
                
            # Exact numeric match
            return actual_val == parse_value(rv)
            
        except ValueError:
            # Fallback for unparseable strings
            return str(actual_val) == rule_val
            
    return False

def check_capture_delay(rule_val, merchant_val):
    """Special handler for capture delay which mixes strings and numeric ranges."""
    if rule_val is None:
        return True
        
    # If both are identical strings (e.g. 'manual' == 'manual')
    if str(rule_val) == str(merchant_val):
        return True
        
    # If merchant value is numeric (e.g. '1', '7'), check against rule range
    try:
        m_days = float(merchant_val)
        return parse_range_check(rule_val, m_days)
    except (ValueError, TypeError):
        return False

def match_fee_rule(tx_ctx, rule):
    """Checks if a transaction context matches a fee rule."""
    # 1. Card Scheme (Exact)
    if rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List contains)
    if rule['account_type'] is not None and len(rule['account_type']) > 0:
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List contains)
    if rule['merchant_category_code'] is not None and len(rule['merchant_category_code']) > 0:
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Bool match)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False
            
    # 5. ACI (List contains)
    if rule['aci'] is not None and len(rule['aci']) > 0:
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Capture Delay (Complex match)
    if not check_capture_delay(rule['capture_delay'], tx_ctx['capture_delay']):
        return False
        
    # 7. Monthly Volume (Range match)
    if not parse_range_check(rule['monthly_volume'], tx_ctx['monthly_volume']):
        return False
        
    # 8. Monthly Fraud Level (Range match)
    if not parse_range_check(rule['monthly_fraud_level'], tx_ctx['monthly_fraud_level']):
        return False
        
    # 9. Intracountry (Bool match)
    if rule['intracountry'] is not None:
        # Convert rule value to bool (0.0 -> False, 1.0 -> True)
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_ctx['intracountry']:
            return False
            
    return True

def execute_step():
    # Load Data
    payments = pd.read_csv('/output/chunk5/data/context/payments.csv')
    with open('/output/chunk5/data/context/merchant_data.json') as f:
        merchant_data = json.load(f)
    with open('/output/chunk5/data/context/fees.json') as f:
        fees = json.load(f)
        
    target_merchant = 'Golfclub_Baron_Friso'
    
    # Filter for July 2023 (Day 182 to 212)
    # 2023 is non-leap. Jan(31)+Feb(28)+Mar(31)+Apr(30)+May(31)+Jun(30) = 181 days.
    # July 1st is Day 182. July 31st is Day 212.
    july_mask = (
        (payments['merchant'] == target_merchant) &
        (payments['year'] == 2023) &
        (payments['day_of_year'] >= 182) &
        (payments['day_of_year'] <= 212)
    )
    july_txs = payments[july_mask].copy()
    
    if july_txs.empty:
        print("0.0")
        return

    # Calculate Monthly Stats (Volume & Fraud) for July
    # Manual says: "Monthly volumes and rates are computed always in natural months"
    monthly_volume = july_txs['eur_amount'].sum()
    
    fraud_txs = july_txs[july_txs['has_fraudulent_dispute'] == True]
    fraud_volume = fraud_txs['eur_amount'].sum()
    
    # Fraud level is ratio of fraud volume to total volume
    monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0
    
    # Get Merchant Metadata
    m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_info:
        print("Error: Merchant not found")
        return
        
    account_type = m_info['account_type']
    mcc = m_info['merchant_category_code']
    capture_delay = m_info['capture_delay']
    
    total_fees = 0.0
    
    # Iterate transactions to calculate fees
    for _, tx in july_txs.iterrows():
        # Determine Intracountry (Issuer == Acquirer)
        is_intra = (tx['issuing_country'] == tx['acquirer_country'])
        
        # Build Context for Rule Matching
        ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': account_type,
            'mcc': mcc,
            'is_credit': bool(tx['is_credit']),
            'aci': tx['aci'],
            'capture_delay': capture_delay,
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_level,
            'intracountry': is_intra
        }
        
        # Find First Matching Rule
        matched_rule = None
        for rule in fees:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break
        
        if matched_rule:
            # Calculate Fee: Fixed + (Rate * Amount / 10000)
            # Rate is an integer variable rate (basis points-like but divisor is 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000.0)
            total_fees += fee
            
    # Print result with high precision
    print(f"{total_fees:.14f}")

if __name__ == "__main__":
    execute_step()

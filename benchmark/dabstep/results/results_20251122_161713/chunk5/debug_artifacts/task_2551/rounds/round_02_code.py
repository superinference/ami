# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2551
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10241 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np
import re

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        
        # Handle percentages
        if '%' in v:
            return float(v.replace('%', '')) / 100
            
        # Handle 'k' (thousands) and 'm' (millions)
        if v.lower().endswith('k'):
            return float(v[:-1]) * 1_000
        if v.lower().endswith('m'):
            return float(v[:-1]) * 1_000_000
            
        # Range handling (e.g., "50-60") - return mean (fallback, though usually we parse ranges explicitly)
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
    return 0.0

def parse_range(rule_value, actual_value_float):
    """
    Checks if actual_value_float fits into rule_value (which can be a range string, inequality, or null).
    Returns True if match, False otherwise.
    """
    if rule_value is None:
        return True
        
    rule_str = str(rule_value).strip()
    
    # Handle inequalities
    if rule_str.startswith('>'):
        limit = coerce_to_float(rule_str[1:])
        return actual_value_float > limit
    if rule_str.startswith('<'):
        limit = coerce_to_float(rule_str[1:])
        return actual_value_float < limit
        
    # Handle ranges "min-max"
    if '-' in rule_str:
        parts = rule_str.split('-')
        if len(parts) == 2:
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            # Inclusive check usually, but let's be standard
            return min_val <= actual_value_float <= max_val
            
    # Exact match (numeric)
    try:
        val = coerce_to_float(rule_str)
        return val == actual_value_float
    except:
        return False

def check_capture_delay(rule_delay, merchant_delay):
    """
    Matches capture delay rules.
    Merchant delay: '1', '2', 'manual', 'immediate'
    Rule delay: '3-5', '>5', '<3', 'immediate', 'manual', None
    """
    if rule_delay is None:
        return True
    
    # String exact matches
    if str(rule_delay) == str(merchant_delay):
        return True
        
    # If merchant delay is numeric (string rep), check against rule ranges
    if str(merchant_delay).isdigit():
        delay_val = float(merchant_delay)
        return parse_range(rule_delay, delay_val)
        
    return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_context must contain:
      - card_scheme, account_type, mcc, is_credit, aci, intracountry
      - monthly_volume, monthly_fraud_rate, capture_delay
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match or wildcard)
    # Rule has list of types. If not empty, merchant's type must be in it.
    if rule.get('account_type') and tx_context['account_type'] not in rule['account_type']:
        return False

    # 3. Merchant Category Code (List match or wildcard)
    # Rule has list of MCCs. If not empty, merchant's MCC must be in it.
    if rule.get('merchant_category_code') and tx_context['mcc'] not in rule['merchant_category_code']:
        return False

    # 4. Is Credit (Boolean match or wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 5. ACI (List match or wildcard)
    if rule.get('aci') and tx_context['aci'] not in rule['aci']:
        return False

    # 6. Intracountry (Boolean match or wildcard)
    if rule.get('intracountry') is not None:
        # Convert rule value to bool if it's string '0.0'/'1.0' or float
        rule_intra = rule['intracountry']
        if isinstance(rule_intra, str):
            rule_intra = (float(rule_intra) == 1.0)
        elif isinstance(rule_intra, (int, float)):
            rule_intra = (rule_intra == 1.0)
            
        if rule_intra != tx_context['intracountry']:
            return False

    # 7. Capture Delay (Complex match)
    if not check_capture_delay(rule.get('capture_delay'), tx_context['capture_delay']):
        return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range(rule['monthly_volume'], tx_context['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        # tx_context['monthly_fraud_rate'] is a float (e.g., 0.083 for 8.3%)
        # rule is string like ">8.3%"
        if not parse_range(rule['monthly_fraud_level'], tx_context['monthly_fraud_rate']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

def main():
    # 1. Load Data
    print("Loading data...")
    payments = pd.read_csv('/output/chunk5/data/context/payments.csv')
    with open('/output/chunk5/data/context/merchant_data.json', 'r') as f:
        merchant_data = json.load(f)
    with open('/output/chunk5/data/context/fees.json', 'r') as f:
        fees = json.load(f)

    # 2. Filter for Rafa_AI and 2023
    target_merchant = 'Rafa_AI'
    target_year = 2023
    
    df = payments[
        (payments['merchant'] == target_merchant) & 
        (payments['year'] == target_year)
    ].copy()
    
    if df.empty:
        print("No transactions found for Rafa_AI in 2023.")
        return

    # 3. Get Static Merchant Data
    m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not m_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return
        
    original_mcc = m_info['merchant_category_code']
    account_type = m_info['account_type']
    capture_delay = m_info['capture_delay']
    
    print(f"Merchant: {target_merchant}")
    print(f"Original MCC: {original_mcc}")
    print(f"Account Type: {account_type}")
    print(f"Capture Delay: {capture_delay}")

    # 4. Calculate Monthly Stats (Volume and Fraud Rate)
    # Convert day_of_year to month
    # 2023 is not a leap year.
    df['date'] = pd.to_datetime(df['year'] * 1000 + df['day_of_year'], format='%Y%j')
    df['month'] = df['date'].dt.month
    
    # Group by month
    monthly_stats = df.groupby('month').agg(
        total_volume=('eur_amount', 'sum'),
        tx_count=('eur_amount', 'count'),
        fraud_count=('has_fraudulent_dispute', 'sum')
    ).reset_index()
    
    monthly_stats['fraud_rate'] = monthly_stats['fraud_count'] / monthly_stats['tx_count']
    
    # Create a lookup dictionary for monthly stats
    # Key: month_int, Value: {vol, fraud_rate}
    stats_lookup = {}
    for _, row in monthly_stats.iterrows():
        stats_lookup[row['month']] = {
            'volume': row['total_volume'],
            'fraud_rate': row['fraud_rate']
        }

    # 5. Calculate Fees
    total_fee_original = 0.0
    total_fee_new = 0.0
    
    # Pre-process fees to ensure numeric types where possible for speed/safety
    # (Already handled in match_fee_rule via helpers)

    print(f"Processing {len(df)} transactions...")
    
    for idx, tx in df.iterrows():
        month = tx['month']
        stats = stats_lookup.get(month)
        
        # Build Context
        context = {
            'card_scheme': tx['card_scheme'],
            'account_type': account_type,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': (tx['issuing_country'] == tx['acquirer_country']),
            'monthly_volume': stats['volume'],
            'monthly_fraud_rate': stats['fraud_rate'],
            'capture_delay': capture_delay,
            # MCC will be set dynamically below
            'mcc': None 
        }
        
        amount = tx['eur_amount']
        
        # --- SCENARIO 1: Original MCC ---
        context['mcc'] = original_mcc
        fee_original = 0.0
        found_original = False
        for rule in fees:
            if match_fee_rule(context, rule):
                fee_original = calculate_fee(amount, rule)
                found_original = True
                break # Stop at first match
        
        if not found_original:
            # Fallback or error? Usually there's a catch-all, but if not, 0.
            # In this dataset, coverage is usually good.
            pass
            
        total_fee_original += fee_original
        
        # --- SCENARIO 2: New MCC (5999) ---
        context['mcc'] = 5999
        fee_new = 0.0
        found_new = False
        for rule in fees:
            if match_fee_rule(context, rule):
                fee_new = calculate_fee(amount, rule)
                found_new = True
                break # Stop at first match
                
        total_fee_new += fee_new

    # 6. Calculate Delta
    # Question: "what amount delta will it have to pay"
    # Usually implies (New - Old).
    delta = total_fee_new - total_fee_original
    
    print(f"Total Fee (Original MCC {original_mcc}): {total_fee_original:.4f}")
    print(f"Total Fee (New MCC 5999): {total_fee_new:.4f}")
    print(f"Delta (New - Old): {delta:.14f}")
    
    # Final Answer Output
    print(f"{delta:.14f}")

if __name__ == "__main__":
    main()

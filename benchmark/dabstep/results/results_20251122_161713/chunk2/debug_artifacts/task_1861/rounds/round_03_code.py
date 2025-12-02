# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1861
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 9488 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators for simple conversion
        if '%' in v:
            return float(v.replace('%', '')) / 100
        return float(v)
    return 0.0

def parse_range_string(range_str):
    """
    Parses strings like '100k-1m', '>5', '7.7%-8.3%' into (min, max) tuple.
    Returns (None, None) if parsing fails or input is None.
    """
    if not range_str or not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle suffixes
    multiplier = 1
    if 'k' in s and 'm' not in s: multiplier = 1_000
    elif 'm' in s and 'k' not in s: multiplier = 1_000_000
    
    is_percent = '%' in s
    
    # Clean string for parsing
    clean_s = s.replace('k', '').replace('m', '').replace('%', '').replace(',', '')
    
    try:
        if '-' in clean_s:
            parts = clean_s.split('-')
            low = float(parts[0])
            high = float(parts[1])
            
            if is_percent:
                low /= 100
                high /= 100
            else:
                # Handle mixed suffixes like "100k-1m"
                # If original string had specific suffixes in specific places
                orig_parts = s.split('-')
                
                # Recalculate low based on its specific suffix
                if 'k' in orig_parts[0]: low = float(parts[0]) * 1_000
                elif 'm' in orig_parts[0]: low = float(parts[0]) * 1_000_000
                else: low = float(parts[0]) * multiplier # Fallback to general multiplier
                
                # Recalculate high based on its specific suffix
                if 'k' in orig_parts[1]: high = float(parts[1]) * 1_000
                elif 'm' in orig_parts[1]: high = float(parts[1]) * 1_000_000
                else: high = float(parts[1]) * multiplier
            
            return low, high
            
        elif '>' in s:
            val = float(clean_s.replace('>', ''))
            if is_percent: val /= 100
            else: val *= multiplier
            return val, float('inf')
            
        elif '<' in s:
            val = float(clean_s.replace('<', ''))
            if is_percent: val /= 100
            else: val *= multiplier
            return float('-inf'), val
            
    except:
        return None, None
        
    return None, None

def check_capture_delay(rule_val, merchant_val):
    """Matches merchant capture delay (e.g., '1') against rule (e.g., '<3')."""
    if rule_val is None:
        return True
    
    # Direct match (strings like 'manual', 'immediate')
    if str(rule_val).lower() == str(merchant_val).lower():
        return True
        
    # If merchant value is non-numeric (e.g. 'manual') but rule is numeric range, return False
    # unless rule is also that string (handled above)
    try:
        m_days = float(merchant_val)
    except (ValueError, TypeError):
        return False

    # Numeric comparison for ranges
    if '-' in str(rule_val):
        low, high = map(float, rule_val.split('-'))
        return low <= m_days <= high
    elif '<' in str(rule_val):
        limit = float(rule_val.replace('<', ''))
        return m_days < limit
    elif '>' in str(rule_val):
        limit = float(rule_val.replace('>', ''))
        return m_days > limit
        
    return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') != tx_context['card_scheme']:
        return False

    # 2. Is Credit (Boolean match, allow None)
    if rule.get('is_credit') is not None:
        # Handle string 'None' or actual None or booleans
        rule_credit = rule['is_credit']
        if str(rule_credit).lower() == 'none':
            pass # Treat as wildcard
        else:
            # Convert to bool for comparison
            r_bool = str(rule_credit).lower() == 'true'
            if r_bool != tx_context['is_credit']:
                return False

    # 3. Intracountry (Boolean match, allow None)
    if rule.get('intracountry') is not None:
        rule_intra_raw = rule['intracountry']
        if str(rule_intra_raw).lower() == 'none':
            pass
        else:
            # fees.json often uses 0.0/1.0 for booleans
            try:
                rule_intra = bool(float(rule_intra_raw))
            except:
                rule_intra = str(rule_intra_raw).lower() == 'true'
            
            if rule_intra != tx_context['is_intracountry']:
                return False

    # 4. Account Type (List match, allow empty/None)
    if rule.get('account_type'): # If list is not empty
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 5. MCC (List match, allow empty/None)
    if rule.get('merchant_category_code'): # If list is not empty
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 6. ACI (List match, allow empty/None)
    if rule.get('aci'): # If list is not empty
        if tx_context['aci'] not in rule['aci']:
            return False

    # 7. Capture Delay (Complex match, allow None)
    if rule.get('capture_delay'):
        if not check_capture_delay(rule['capture_delay'], tx_context['capture_delay']):
            return False

    # 8. Monthly Volume (Range match, allow None)
    if rule.get('monthly_volume'):
        low, high = parse_range_string(rule['monthly_volume'])
        if low is not None:
            vol = tx_context['monthly_volume']
            if not (low <= vol <= high):
                return False

    # 9. Monthly Fraud Level (Range match, allow None)
    if rule.get('monthly_fraud_level'):
        low, high = parse_range_string(rule['monthly_fraud_level'])
        if low is not None:
            fraud = tx_context['monthly_fraud_rate']
            if not (low <= fraud <= high):
                return False

    return True

# ==========================================
# MAIN SCRIPT
# ==========================================

# File paths
payments_path = '/output/chunk2/data/context/payments.csv'
merchant_path = '/output/chunk2/data/context/merchant_data.json'
fees_path = '/output/chunk2/data/context/fees.json'

# 1. Load Data
print("Loading data...")
df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Filter for Rafa_AI in March 2023
# March 2023 (non-leap) is Day 60 to 90
target_merchant = 'Rafa_AI'
df_march = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == 2023) & 
    (df_payments['day_of_year'] >= 60) & 
    (df_payments['day_of_year'] <= 90)
].copy()

print(f"Found {len(df_march)} transactions for {target_merchant} in March 2023.")

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

print(f"Merchant Metadata: {merchant_info}")

# 4. Calculate Monthly Stats (Volume & Fraud Rate)
monthly_volume = df_march['eur_amount'].sum()
monthly_fraud_count = df_march['has_fraudulent_dispute'].sum()
monthly_tx_count = len(df_march)
monthly_fraud_rate = monthly_fraud_count / monthly_tx_count if monthly_tx_count > 0 else 0.0

print(f"Monthly Volume: €{monthly_volume:,.2f}")
print(f"Monthly Fraud Rate: {monthly_fraud_rate:.4%}")

# 5. Calculate Fees per Transaction
total_fees = 0.0
matched_count = 0
unmatched_count = 0

# Pre-calculate context constant for this merchant/month
base_context = {
    'account_type': merchant_info['account_type'],
    'mcc': merchant_info['merchant_category_code'],
    'capture_delay': merchant_info['capture_delay'],
    'monthly_volume': monthly_volume,
    'monthly_fraud_rate': monthly_fraud_rate
}

print("Calculating fees...")

for _, row in df_march.iterrows():
    # Build transaction-specific context
    tx_context = base_context.copy()
    tx_context['card_scheme'] = row['card_scheme']
    tx_context['is_credit'] = row['is_credit']
    tx_context['aci'] = row['aci']
    tx_context['is_intracountry'] = (row['issuing_country'] == row['acquirer_country'])
    
    # Find matching rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Fee formula: fixed_amount + (rate * amount / 10000)
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * row['eur_amount'] / 10000)
        total_fees += fee
        matched_count += 1
    else:
        unmatched_count += 1

print(f"\nProcessing Complete.")
print(f"Matched Transactions: {matched_count}")
print(f"Unmatched Transactions: {unmatched_count}")
print(f"Total Fees Paid by {target_merchant} in March 2023: €{total_fees:.2f}")

# Final Output
print(f"{total_fees:.2f}")

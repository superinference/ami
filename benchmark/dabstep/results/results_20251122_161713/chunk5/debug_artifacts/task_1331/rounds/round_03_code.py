# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1331
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: insufficient
# Plan steps: 1
# Code length: 9579 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'k' in v:
            return float(v.replace('k', '')) * 1000
        if 'm' in v:
            return float(v.replace('m', '')) * 1000000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range_check(value, rule_string):
    """
    Check if a numeric value fits within a rule string (e.g., '100k-1m', '>5', '<3').
    Returns True/False.
    """
    if rule_string is None:
        return True
    
    # Handle explicit None string
    if str(rule_string).lower() == 'none':
        return True

    try:
        val = float(value)
    except (ValueError, TypeError):
        return False

    s = str(rule_string).strip().lower()
    
    # Range "min-max"
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return min_val <= val <= max_val
            
    # Inequality ">X"
    if s.startswith('>'):
        limit = coerce_to_float(s[1:])
        return val > limit
        
    # Inequality "<X"
    if s.startswith('<'):
        limit = coerce_to_float(s[1:])
        return val < limit
        
    # Exact match (numeric)
    try:
        rule_val = coerce_to_float(s)
        return val == rule_val
    except:
        return False

def match_fee_rule(ctx, rule):
    """
    Check if a transaction context matches a fee rule.
    ctx: dict of transaction/merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') != ctx.get('card_scheme'):
        return False

    # 2. Account Type (List match or Empty/Wildcard)
    if rule.get('account_type'):
        if ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. MCC (List match or Empty/Wildcard)
    if rule.get('merchant_category_code'):
        if ctx.get('merchant_category_code') not in rule['merchant_category_code']:
            return False

    # 4. ACI (List match or Empty/Wildcard)
    if rule.get('aci'):
        if ctx.get('aci') not in rule['aci']:
            return False

    # 5. Is Credit (Bool match or None/Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx.get('is_credit'):
            return False

    # 6. Intracountry (Bool match or None/Wildcard)
    # Note: fees.json uses 0.0/1.0/None for intracountry sometimes, or boolean
    if rule.get('intracountry') is not None:
        rule_intra = rule['intracountry']
        # Normalize rule value to bool
        if isinstance(rule_intra, (int, float)):
            rule_intra = bool(rule_intra)
        elif isinstance(rule_intra, str):
            rule_intra = (rule_intra.lower() == 'true')
        
        if rule_intra != ctx.get('intracountry'):
            return False

    # 7. Capture Delay (String/Range match or None)
    if rule.get('capture_delay'):
        # If rule is exact string match (e.g. 'manual')
        if rule['capture_delay'] == ctx.get('capture_delay'):
            pass # Match
        # If rule is numeric range (e.g. '<3') and context is numeric-like
        elif any(c.isdigit() for c in str(ctx.get('capture_delay'))) and any(c in '<>=' for c in str(rule['capture_delay'])):
             if not parse_range_check(coerce_to_float(ctx['capture_delay']), rule['capture_delay']):
                 return False
        # If rule is range but context is text (e.g. rule '<3', ctx 'manual') -> Mismatch
        elif rule['capture_delay'] != ctx.get('capture_delay'):
             return False

    # 8. Monthly Volume (Range match or None)
    if rule.get('monthly_volume'):
        if not parse_range_check(ctx.get('monthly_volume'), rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match or None)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(ctx.get('monthly_fraud_level'), rule['monthly_fraud_level']):
            return False

    return True

def get_month_from_doy(doy):
    """Map day of year (1-365) to month (1-12) for non-leap year."""
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cumulative = 0
    for i, days in enumerate(days_in_months):
        cumulative += days
        if doy <= cumulative:
            return i + 1
    return 12

# ==========================================
# MAIN ANALYSIS
# ==========================================

def main():
    # 1. Load Data
    try:
        df_mcc = pd.read_csv('/output/chunk5/data/context/merchant_category_codes.csv')
        df_payments = pd.read_csv('/output/chunk5/data/context/payments.csv')
        with open('/output/chunk5/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
        with open('/output/chunk5/data/context/fees.json', 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Identify MCC
    target_desc = "Eating Places and Restaurants"
    mcc_match = df_mcc[df_mcc['description'].str.contains(target_desc, case=False, na=False)]
    
    if mcc_match.empty:
        print("MCC not found")
        return
    
    target_mcc = int(mcc_match.iloc[0]['mcc'])
    # print(f"Target MCC: {target_mcc}")

    # 3. Identify Merchants (Account Type H + MCC)
    target_merchants = []
    merchant_configs = {}
    
    for m in merchant_data:
        if m.get('account_type') == 'H' and m.get('merchant_category_code') == target_mcc:
            target_merchants.append(m['merchant'])
            merchant_configs[m['merchant']] = m
            
    if not target_merchants:
        print("No matching merchants found.")
        return
    
    # print(f"Target Merchants: {target_merchants}")

    # 4. Calculate Monthly Stats for these merchants
    # We need this to determine which fee rule applies (volume/fraud brackets)
    df_payments['month'] = df_payments['day_of_year'].apply(get_month_from_doy)
    
    # Filter for ALL transactions of these merchants to get accurate volume/fraud stats
    df_merchant_txs = df_payments[df_payments['merchant'].isin(target_merchants)].copy()
    
    # Group by merchant and month
    monthly_stats = df_merchant_txs.groupby(['merchant', 'month']).agg(
        total_volume=('eur_amount', 'sum'),
        fraud_count=('has_fraudulent_dispute', 'sum'),
        tx_count=('psp_reference', 'count')
    ).reset_index()
    
    monthly_stats['fraud_rate'] = monthly_stats['fraud_count'] / monthly_stats['tx_count']
    
    # Create lookup: (merchant, month) -> (volume, fraud_rate)
    stats_lookup = {}
    for _, row in monthly_stats.iterrows():
        stats_lookup[(row['merchant'], row['month'])] = (row['total_volume'], row['fraud_rate'])

    # 5. Filter for SwiftCharge Transactions
    # The question asks about SwiftCharge fees
    df_swift = df_merchant_txs[df_merchant_txs['card_scheme'] == 'SwiftCharge'].copy()
    
    if df_swift.empty:
        print("No SwiftCharge transactions found for these merchants.")
        return

    # 6. Calculate Hypothetical Fees
    hypothetical_amount = 1234.0
    calculated_fees = []
    
    for _, tx in df_swift.iterrows():
        merch = tx['merchant']
        month = tx['month']
        
        # Retrieve monthly stats for this transaction's context
        vol, fraud = stats_lookup.get((merch, month), (0, 0))
        m_config = merchant_configs[merch]
        
        # Build Context
        ctx = {
            'card_scheme': 'SwiftCharge',
            'account_type': m_config['account_type'],
            'merchant_category_code': m_config['merchant_category_code'],
            'capture_delay': m_config['capture_delay'],
            'monthly_volume': vol,
            'monthly_fraud_level': fraud,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'intracountry': (tx['issuing_country'] == tx['acquirer_country'])
        }
        
        # Find Matching Rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break # Assume first match wins (standard rule engine logic)
        
        if matched_rule:
            # Calculate Fee: Fixed + (Rate * Amount / 10000)
            # Rate is in basis points (per 10,000) usually, or specified as integer to be divided
            # fees.json documentation says: "rate: integer... multiplied by transaction value and divided by 10000"
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * hypothetical_amount / 10000.0)
            calculated_fees.append(fee)
        else:
            # If no rule matches, we skip (or could flag error)
            pass

    # 7. Compute Average
    if calculated_fees:
        avg_fee = sum(calculated_fees) / len(calculated_fees)
        print(f"{avg_fee:.6f}")
    else:
        print("No applicable fee rules found for transactions.")

if __name__ == "__main__":
    main()

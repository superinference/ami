import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float."""
    if value is None or pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().lower().replace(',', '').replace('€', '').replace('$', '')
        v = v.replace('>', '').replace('<', '').replace('≥', '').replace('≤', '')
        if '%' in v:
            return float(v.replace('%', '')) / 100.0
        if 'k' in v:
            return float(v.replace('k', '')) * 1000
        if 'm' in v:
            return float(v.replace('m', '')) * 1000000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return 0.0

def parse_range_check(value, rule_str):
    """Check if a numeric value falls within a rule string range (e.g., '100k-1m', '>5')."""
    if rule_str is None:
        return True
    
    # Handle specific strings like 'immediate', 'manual'
    if isinstance(value, str) and isinstance(rule_str, str):
        if rule_str.lower() in ['immediate', 'manual']:
            return value.lower() == rule_str.lower()

    # Numeric handling
    try:
        val_num = float(value)
    except (ValueError, TypeError):
        # If value is not numeric (e.g. 'immediate') but rule is numeric/range, it's a mismatch
        return False

    s = str(rule_str).strip().lower()
    
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return min_val <= val_num <= max_val
    elif '>' in s:
        limit = coerce_to_float(s.replace('>', ''))
        return val_num > limit
    elif '<' in s:
        limit = coerce_to_float(s.replace('<', ''))
        return val_num < limit
    
    # Exact numeric match fallback
    return val_num == coerce_to_float(s)

def match_fee_rule(tx_ctx, rule):
    """
    Check if a transaction context matches a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List match or Wildcard)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match or Wildcard)
    if rule.get('merchant_category_code'):
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 4. ACI (List match or Wildcard)
    # CRITICAL: We use the SIMULATED ACI here
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 5. Is Credit (Boolean match or Wildcard)
    if rule.get('is_credit') is not None:
        if bool(rule['is_credit']) != bool(tx_ctx['is_credit']):
            return False

    # 6. Intracountry (Boolean match or Wildcard)
    if rule.get('intracountry') is not None:
        # fees.json uses 0.0/1.0 or boolean for intracountry
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['is_intracountry']:
            return False

    # 7. Capture Delay (Range/String match or Wildcard)
    if rule.get('capture_delay'):
        if not parse_range_check(tx_ctx['capture_delay'], rule['capture_delay']):
            return False

    # 8. Monthly Volume (Range match or Wildcard)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match or Wildcard)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_ctx['monthly_fraud_level'], rule['monthly_fraud_level']):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculate fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0)
    rate = rule.get('rate', 0.0)
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000.0)

def solve():
    # --- 1. Load Data ---
    payments_path = '/output/chunk6/data/context/payments.csv'
    fees_path = '/output/chunk6/data/context/fees.json'
    merchant_data_path = '/output/chunk6/data/context/merchant_data.json'

    df = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_data_path, 'r') as f:
        merchant_data_list = json.load(f)

    # --- 2. Filter Context (Martinis, April) ---
    target_merchant = "Martinis_Fine_Steakhouse"
    
    # April is Day 91 to 120 (Non-leap year 2023)
    df_merchant = df[df['merchant'] == target_merchant]
    df_april = df_merchant[(df_merchant['day_of_year'] >= 91) & (df_merchant['day_of_year'] <= 120)]

    if df_april.empty:
        print("No transactions found for merchant in April.")
        return

    # --- 3. Calculate Merchant Metrics (for Fee Rules) ---
    # These metrics apply to the merchant for the whole month and determine which fee tier applies
    monthly_volume = df_april['eur_amount'].sum()
    
    # Fraud Rate = Fraud Volume / Total Volume
    fraud_txs_all = df_april[df_april['has_fraudulent_dispute'] == True]
    fraud_volume_all = fraud_txs_all['eur_amount'].sum()
    
    if monthly_volume > 0:
        monthly_fraud_level = fraud_volume_all / monthly_volume # Ratio (e.g. 0.08)
    else:
        monthly_fraud_level = 0.0

    # Get Merchant Static Attributes
    merchant_info = next((m for m in merchant_data_list if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return

    account_type = merchant_info['account_type']
    mcc = merchant_info['merchant_category_code']
    capture_delay = merchant_info['capture_delay']

    # --- 4. Isolate Fraudulent Transactions for Simulation ---
    # We only want to optimize fees for the fraudulent transactions
    df_fraud_target = df_april[df_april['has_fraudulent_dispute'] == True].copy()
    
    if df_fraud_target.empty:
        print("No fraudulent transactions to optimize.")
        return

    # --- 5. Simulate ACI Changes ---
    candidate_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    results = {}

    # Pre-calculate static context for each transaction to speed up loop
    tx_contexts = []
    for _, row in df_fraud_target.iterrows():
        is_intra = (row['issuing_country'] == row['acquirer_country'])
        ctx = {
            'card_scheme': row['card_scheme'],
            'is_credit': row['is_credit'],
            'eur_amount': row['eur_amount'],
            'is_intracountry': is_intra,
            'account_type': account_type,
            'merchant_category_code': mcc,
            'capture_delay': capture_delay,
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_level,
            # 'aci': will be set in the loop
        }
        tx_contexts.append(ctx)

    # Iterate through each candidate ACI
    for aci in candidate_acis:
        total_fee_for_aci = 0.0
        
        for ctx in tx_contexts:
            # Set the simulated ACI
            simulated_ctx = ctx.copy()
            simulated_ctx['aci'] = aci
            
            # Find applicable fee
            matched_rule = None
            for rule in fees_data:
                if match_fee_rule(simulated_ctx, rule):
                    matched_rule = rule
                    break
            
            if matched_rule:
                fee = calculate_fee(simulated_ctx['eur_amount'], matched_rule)
                total_fee_for_aci += fee
            else:
                # Fallback if no rule matches (should not happen with complete fee tables)
                # Assign a high penalty to avoid selecting this ACI
                total_fee_for_aci += 1e9

        results[aci] = total_fee_for_aci

    # --- 6. Determine Preferred Choice ---
    # Find ACI with minimum total fee
    best_aci = min(results, key=results.get)
    
    # Output the result
    print(best_aci)

if __name__ == "__main__":
    solve()
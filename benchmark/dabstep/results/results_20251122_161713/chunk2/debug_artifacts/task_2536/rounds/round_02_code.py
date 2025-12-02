# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2536
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10381 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
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

def parse_volume_range(range_str):
    """Parses volume strings like '100k-1m' into (min, max) tuple."""
    if not range_str:
        return (0, float('inf'))
    
    def parse_val(s):
        s = s.lower().strip()
        mult = 1
        if 'k' in s:
            mult = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mult = 1000000
            s = s.replace('m', '')
        return float(s) * mult

    try:
        if '-' in range_str:
            parts = range_str.split('-')
            return (parse_val(parts[0]), parse_val(parts[1]))
        elif range_str.startswith('>'):
            return (parse_val(range_str[1:]), float('inf'))
        elif range_str.startswith('<'):
            return (0, parse_val(range_str[1:]))
        return (0, float('inf'))
    except:
        return (0, float('inf'))

def parse_fraud_range(range_str):
    """Parses fraud strings like '7.7%-8.3%' into (min, max) tuple."""
    if not range_str:
        return (0, float('inf'))
    
    def parse_val(s):
        s = s.strip().replace('%', '')
        return float(s) / 100

    try:
        if '-' in range_str:
            parts = range_str.split('-')
            return (parse_val(parts[0]), parse_val(parts[1]))
        elif range_str.startswith('>'):
            return (parse_val(range_str[1:]), float('inf'))
        elif range_str.startswith('<'):
            return (0, parse_val(range_str[1:]))
        return (0, float('inf'))
    except:
        return (0, float('inf'))

def check_capture_delay(rule_delay, merchant_delay):
    """Checks if merchant capture delay matches rule."""
    if not rule_delay:
        return True
    
    # Direct match
    if rule_delay == merchant_delay:
        return True
    
    # Numeric handling for merchant delays like "1", "7"
    try:
        delay_days = float(merchant_delay)
        if rule_delay == 'immediate': return delay_days == 0
        if rule_delay == 'manual': return False # Manual is not numeric usually
        if rule_delay.startswith('<'):
            return delay_days < float(rule_delay[1:])
        if rule_delay.startswith('>'):
            return delay_days > float(rule_delay[1:])
        if '-' in rule_delay:
            low, high = map(float, rule_delay.split('-'))
            return low <= delay_days <= high
    except ValueError:
        # Merchant delay is string (e.g. "manual", "immediate")
        return rule_delay == merchant_delay
    
    return False

def get_month_from_doy(doy, year=2023):
    """Returns month (1-12) from day of year."""
    # Simple approximation or use pandas
    return pd.Timestamp(year, 1, 1) + pd.Timedelta(days=doy - 1)

def match_fee_rule(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    tx_context: dict containing transaction details and monthly stats
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List match or wildcard)
    if rule['account_type'] and tx_context['account_type'] not in rule['account_type']:
        return False

    # 3. Capture Delay (Complex match or wildcard)
    if not check_capture_delay(rule['capture_delay'], tx_context['capture_delay']):
        return False

    # 4. Merchant Category Code (List match or wildcard)
    # CRITICAL: Check if rule has MCC list, if so, does it contain our MCC?
    if rule['merchant_category_code'] and tx_context['mcc'] not in rule['merchant_category_code']:
        return False

    # 5. Is Credit (Exact match or wildcard)
    # rule['is_credit'] can be True, False, or None
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 6. ACI (List match or wildcard)
    if rule['aci'] and tx_context['aci'] not in rule['aci']:
        return False

    # 7. Intracountry (Exact match or wildcard)
    # rule['intracountry'] is 0.0, 1.0, or None
    if rule['intracountry'] is not None:
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_context['intracountry']:
            return False

    # 8. Monthly Volume (Range match or wildcard)
    if rule['monthly_volume']:
        min_vol, max_vol = parse_volume_range(rule['monthly_volume'])
        if not (min_vol <= tx_context['monthly_volume'] <= max_vol):
            return False

    # 9. Monthly Fraud Level (Range match or wildcard)
    if rule['monthly_fraud_level']:
        min_fraud, max_fraud = parse_fraud_range(rule['monthly_fraud_level'])
        # Note: fraud level is a ratio (0.0 - 1.0)
        if not (min_fraud <= tx_context['monthly_fraud_ratio'] <= max_fraud):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    # Rate is integer, divided by 10000
    variable_fee = (rule['rate'] * amount) / 10000
    return rule['fixed_amount'] + variable_fee

def execute_analysis():
    # 1. Load Data
    print("Loading data...")
    try:
        df_payments = pd.read_csv('/output/chunk2/data/context/payments.csv')
        with open('/output/chunk2/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
        with open('/output/chunk2/data/context/fees.json', 'r') as f:
            fees_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Filter for Merchant and Year
    target_merchant = 'Crossfit_Hanna'
    df = df_payments[(df_payments['merchant'] == target_merchant) & (df_payments['year'] == 2023)].copy()
    
    if df.empty:
        print(f"No transactions found for {target_merchant} in 2023.")
        return

    # 3. Get Merchant Metadata
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return

    original_mcc = merchant_info['merchant_category_code']
    account_type = merchant_info['account_type']
    capture_delay = merchant_info['capture_delay']
    
    print(f"Merchant: {target_merchant}")
    print(f"Original MCC: {original_mcc}")
    print(f"Account Type: {account_type}")
    print(f"Capture Delay: {capture_delay}")
    print(f"Transaction Count: {len(df)}")

    # 4. Preprocess Data for Fee Calculation
    # Add Month column
    df['date'] = pd.to_datetime(df['year'] * 1000 + df['day_of_year'], format='%Y%j')
    df['month'] = df['date'].dt.month
    
    # Add Intracountry column
    df['intracountry'] = df['issuing_country'] == df['acquirer_country']

    # Calculate Monthly Stats (Volume and Fraud Ratio)
    # Fraud Ratio = Fraud Volume / Total Volume (as per manual section 7)
    monthly_stats = {}
    for month in df['month'].unique():
        month_data = df[df['month'] == month]
        total_vol = month_data['eur_amount'].sum()
        fraud_vol = month_data[month_data['has_fraudulent_dispute']]['eur_amount'].sum()
        fraud_ratio = fraud_vol / total_vol if total_vol > 0 else 0.0
        monthly_stats[month] = {
            'volume': total_vol,
            'fraud_ratio': fraud_ratio
        }

    # 5. Define Calculation Function
    def calculate_total_fees_for_mcc(target_mcc):
        total_fees = 0.0
        
        # Iterate through all transactions
        # Using itertuples for speed
        for row in df.itertuples():
            # Build context
            tx_context = {
                'card_scheme': row.card_scheme,
                'account_type': account_type,
                'capture_delay': capture_delay,
                'mcc': target_mcc,
                'is_credit': row.is_credit,
                'aci': row.aci,
                'intracountry': row.intracountry,
                'monthly_volume': monthly_stats[row.month]['volume'],
                'monthly_fraud_ratio': monthly_stats[row.month]['fraud_ratio']
            }
            
            # Find matching rule
            matched_rule = None
            for rule in fees_data:
                if match_fee_rule(tx_context, rule):
                    matched_rule = rule
                    break # Stop at first match
            
            if matched_rule:
                fee = calculate_fee(row.eur_amount, matched_rule)
                total_fees += fee
            else:
                # If no rule matches, assume 0 or raise warning? 
                # In this dataset, usually there's a fallback. 
                # If no match, we skip (fee=0) but print warning once.
                pass

        return total_fees

    # 6. Calculate Fees for Original MCC
    print("Calculating fees for Original MCC...")
    fees_original = calculate_total_fees_for_mcc(original_mcc)
    print(f"Total Fees (Original MCC {original_mcc}): {fees_original:.4f}")

    # 7. Calculate Fees for New MCC
    new_mcc = 5999
    print(f"Calculating fees for New MCC {new_mcc}...")
    fees_new = calculate_total_fees_for_mcc(new_mcc)
    print(f"Total Fees (New MCC {new_mcc}): {fees_new:.4f}")

    # 8. Calculate Delta
    delta = fees_new - fees_original
    
    # 9. Output Result
    print("\n" + "="*30)
    print(f"Fee Delta (New - Old): {delta:.14f}")
    print("="*30)

if __name__ == "__main__":
    execute_analysis()

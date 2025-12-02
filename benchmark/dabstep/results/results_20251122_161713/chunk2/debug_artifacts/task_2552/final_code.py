import pandas as pd
import json
import numpy as np

# --- Helper Functions ---

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
            try:
                return float(v.replace('%', '')) / 100
            except:
                return 0.0
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
            return 0.0
    return float(value)

def parse_range(range_str):
    """Parses a string range like '100k-1m', '<3', '>5', '7.7%-8.3%' into a tuple (min, max)."""
    if range_str is None:
        return (-float('inf'), float('inf'))
    
    s = str(range_str).strip().lower()
    
    # Handle percentages
    is_percent = '%' in s
    s = s.replace('%', '')
    
    # Handle k/m suffixes for volume
    def parse_val(v):
        v = v.strip()
        mult = 1
        if v.endswith('k'):
            mult = 1000
            v = v[:-1]
        elif v.endswith('m'):
            mult = 1000000
            v = v[:-1]
        try:
            val = float(v)
            return val * mult
        except:
            return 0.0

    # Determine scale (percentages are 0.0-1.0, others are raw)
    scale = 0.01 if is_percent else 1.0

    if '-' in s:
        try:
            parts = s.split('-')
            return (parse_val(parts[0]) * scale, parse_val(parts[1]) * scale)
        except:
            return (-float('inf'), float('inf'))
    elif s.startswith('<'):
        return (-float('inf'), parse_val(s[1:]) * scale)
    elif s.startswith('>'):
        return (parse_val(s[1:]) * scale, float('inf'))
    else:
        # Exact match treated as range
        try:
            val = parse_val(s) * scale
            return (val, val)
        except:
             return (-float('inf'), float('inf'))

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches rule."""
    if rule_delay is None:
        return True
    
    md = str(merchant_delay).lower()
    rd = str(rule_delay).lower()
    
    # Handle exact string matches
    if rd == 'immediate':
        return md == 'immediate'
    if rd == 'manual':
        return md == 'manual'
    
    # Handle numeric comparisons
    try:
        days = float(md)
    except ValueError:
        # If merchant is 'immediate' or 'manual' but rule is numeric range, no match
        return False
        
    if '-' in rd:
        try:
            low, high = map(float, rd.split('-'))
            return low <= days <= high
        except:
            return False
    elif rd.startswith('<'):
        try:
            return days < float(rd[1:])
        except:
            return False
    elif rd.startswith('>'):
        try:
            return days > float(rd[1:])
        except:
            return False
    
    return False

def calculate_fee_amount(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = float(rule['fixed_amount'])
    rate = float(rule['rate'])
    # Manual: fee = fixed_amount + rate * transaction_value / 10000
    return fixed + (rate * amount / 10000)

def get_month(day_of_year):
    """Maps day of year (1-365) to month (1-12) for 2023."""
    # 2023 is not a leap year
    if day_of_year <= 31: return 1
    if day_of_year <= 59: return 2
    if day_of_year <= 90: return 3
    if day_of_year <= 120: return 4
    if day_of_year <= 151: return 5
    if day_of_year <= 181: return 6
    if day_of_year <= 212: return 7
    if day_of_year <= 243: return 8
    if day_of_year <= 273: return 9
    if day_of_year <= 304: return 10
    if day_of_year <= 334: return 11
    return 12

# --- Main Analysis ---

def main():
    # 1. Load Data
    payments_path = '/output/chunk2/data/context/payments.csv'
    fees_path = '/output/chunk2/data/context/fees.json'
    merchant_path = '/output/chunk2/data/context/merchant_data.json'

    df = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)

    # 2. Filter for Rafa_AI
    merchant_name = 'Rafa_AI'
    df_rafa = df[df['merchant'] == merchant_name].copy()

    if df_rafa.empty:
        print("No transactions found for Rafa_AI")
        return

    # 3. Get Merchant Context
    rafa_info = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
    if not rafa_info:
        raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

    original_mcc = rafa_info['merchant_category_code']
    account_type = rafa_info['account_type']
    capture_delay = rafa_info['capture_delay']
    new_mcc = 7523

    # 4. Calculate Monthly Stats (Volume and Fraud)
    df_rafa['month'] = df_rafa['day_of_year'].apply(get_month)

    monthly_stats = {}
    for month in range(1, 13):
        month_txs = df_rafa[df_rafa['month'] == month]
        if len(month_txs) == 0:
            monthly_stats[month] = {'vol': 0.0, 'fraud_rate': 0.0}
            continue
        
        total_vol = month_txs['eur_amount'].sum()
        # Fraud volume: sum of amounts where has_fraudulent_dispute is True
        fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum()
        
        # Manual: "Fraud is defined as the ratio of fraudulent volume over total volume"
        fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
        monthly_stats[month] = {'vol': total_vol, 'fraud_rate': fraud_rate}

    # 5. Pre-process Fees (Parse ranges once)
    processed_fees = []
    for rule in fees:
        r = rule.copy()
        r['vol_range'] = parse_range(rule['monthly_volume'])
        r['fraud_range'] = parse_range(rule['monthly_fraud_level'])
        processed_fees.append(r)

    # 6. Calculate Fees
    total_fee_original = 0.0
    total_fee_new = 0.0

    # Pre-calculate intracountry for all transactions
    # Manual: "True if the issuer country and the acquiring country are the same"
    df_rafa['intracountry'] = df_rafa['issuing_country'] == df_rafa['acquirer_country']

    # Iterate by month to optimize rule filtering
    for month in range(1, 13):
        month_txs = df_rafa[df_rafa['month'] == month]
        if len(month_txs) == 0:
            continue
            
        stats = monthly_stats[month]
        vol = stats['vol']
        fr = stats['fraud_rate']
        
        # Filter rules applicable to this month's stats (Volume and Fraud)
        month_applicable_rules = []
        for rule in processed_fees:
            # Check Volume
            v_min, v_max = rule['vol_range']
            if not (v_min <= vol <= v_max):
                continue
            # Check Fraud
            f_min, f_max = rule['fraud_range']
            if not (f_min <= fr <= f_max):
                continue
            month_applicable_rules.append(rule)
            
        # Process transactions in this month
        for _, tx in month_txs.iterrows():
            tx_scheme = tx['card_scheme']
            tx_credit = tx['is_credit']
            tx_aci = tx['aci']
            tx_intra = tx['intracountry']
            tx_amt = tx['eur_amount']
            
            def find_fee(target_mcc):
                for rule in month_applicable_rules:
                    # 1. Card Scheme
                    if rule['card_scheme'] != tx_scheme:
                        continue
                    # 2. Account Type (Merchant property) - Wildcard [] matches all
                    if rule['account_type'] and account_type not in rule['account_type']:
                        continue
                    # 3. Capture Delay (Merchant property) - Wildcard None matches all
                    if not check_capture_delay(capture_delay, rule['capture_delay']):
                        continue
                    # 4. MCC (Target MCC) - Wildcard [] matches all
                    if rule['merchant_category_code'] and target_mcc not in rule['merchant_category_code']:
                        continue
                    # 5. Is Credit - Wildcard None matches all
                    if rule['is_credit'] is not None and rule['is_credit'] != tx_credit:
                        continue
                    # 6. ACI - Wildcard [] matches all
                    if rule['aci'] and tx_aci not in rule['aci']:
                        continue
                    # 7. Intracountry - Wildcard None matches all
                    if rule['intracountry'] is not None:
                        rule_intra = bool(float(rule['intracountry']))
                        if rule_intra != tx_intra:
                            continue
                    
                    # Match found
                    return calculate_fee_amount(tx_amt, rule)
                return 0.0 # Should not happen with complete rules

            # Calculate for both scenarios
            fee_orig = find_fee(original_mcc)
            fee_new = find_fee(new_mcc)
            
            total_fee_original += fee_orig
            total_fee_new += fee_new

    # 7. Calculate Delta
    delta = total_fee_new - total_fee_original

    # Print result with high precision
    print(f"{delta:.14f}")

if __name__ == "__main__":
    main()
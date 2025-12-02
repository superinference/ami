import pandas as pd
import json
import re
from datetime import datetime

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators for simple float conversion
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return None
        try:
            return float(v)
        except ValueError:
            return None
    return None

def parse_range_value(value_str):
    """Parses strings like '100k-1m', '>5%', '<3' into (min, max) tuple."""
    if not isinstance(value_str, str):
        return None, None
    
    s = value_str.lower().strip().replace(',', '').replace('€', '').replace('$', '')
    is_percent = '%' in value_str
    s = s.replace('%', '')
    
    # Handle k/m suffixes
    def parse_num(n):
        try:
            if 'k' in n: return float(n.replace('k', '')) * 1000
            if 'm' in n: return float(n.replace('m', '')) * 1000000
            return float(n)
        except:
            return 0.0

    try:
        if '-' in s:
            parts = s.split('-')
            low = parse_num(parts[0])
            high = parse_num(parts[1])
            if is_percent:
                low /= 100
                high /= 100
            return low, high
        elif s.startswith('>'):
            val = parse_num(s[1:])
            if is_percent: val /= 100
            return val, float('inf')
        elif s.startswith('<'):
            val = parse_num(s[1:])
            if is_percent: val /= 100
            return float('-inf'), val
        else:
            # Exact value treated as range [val, val]
            val = parse_num(s)
            if is_percent: val /= 100
            return val, val
    except:
        return None, None

def get_month_from_doy(year, doy):
    """Convert day of year to month (1-12)."""
    try:
        date = datetime.strptime(f'{year}-{doy}', '%Y-%j')
        return date.month
    except ValueError:
        return None

def match_fee_rule(tx_context, rule):
    """
    Checks if a fee rule applies to a transaction context.
    tx_context must contain:
        - card_scheme (str)
        - account_type (str)
        - mcc (int)
        - is_credit (bool)
        - aci (str)
        - intracountry (bool)
        - capture_delay (str)
        - monthly_volume (float)
        - monthly_fraud_rate (float)
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False

    # 2. Account Type (List containment or wildcard)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List containment or wildcard)
    if rule.get('merchant_category_code'):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Exact match or wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False

    # 5. ACI (List containment or wildcard)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False

    # 6. Intracountry (Exact match or wildcard)
    if rule.get('intracountry') is not None:
        # Convert boolean to float 0.0/1.0 if rule uses 0.0/1.0, or compare bools
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_context['intracountry']:
            return False

    # 7. Capture Delay (Exact match or wildcard)
    if rule.get('capture_delay'):
        rule_cd = str(rule['capture_delay'])
        merch_cd = str(tx_context['capture_delay'])
        
        # Handle categorical matches (manual, immediate)
        if merch_cd in ['manual', 'immediate']:
            if rule_cd != merch_cd:
                return False
        # Handle numeric matches if merchant has numeric delay (unlikely for Crossfit_Hanna)
        else:
            # If rule is range (e.g. >5) and merchant is numeric
            if any(c in rule_cd for c in ['<', '>', '-']):
                # Simplified: if merchant is numeric string, we could parse.
                # But Crossfit_Hanna is 'manual', so we skip complex numeric logic here.
                if rule_cd != merch_cd:
                    return False
            else:
                if rule_cd != merch_cd:
                    return False

    # 8. Monthly Volume (Range check)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range_value(rule['monthly_volume'])
        if min_v is not None:
            vol = tx_context['monthly_volume']
            if not (min_v <= vol <= max_v):
                return False

    # 9. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range_value(rule['monthly_fraud_level'])
        if min_f is not None:
            fraud = tx_context['monthly_fraud_rate']
            if not (min_f <= fraud <= max_f):
                return False

    return True

def execute_step():
    # Paths
    payments_path = '/output/chunk4/data/context/payments.csv'
    fees_path = '/output/chunk4/data/context/fees.json'
    merchant_path = '/output/chunk4/data/context/merchant_data.json'

    # 1. Load Data
    try:
        df_payments = pd.read_csv(payments_path)
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        with open(merchant_path, 'r') as f:
            merchant_data_list = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Filter for Target Merchant and Year
    target_merchant = 'Crossfit_Hanna'
    target_year = 2023
    
    df_filtered = df_payments[
        (df_payments['merchant'] == target_merchant) & 
        (df_payments['year'] == target_year)
    ].copy()
    
    if df_filtered.empty:
        print("No transactions found for Crossfit_Hanna in 2023.")
        return

    # 3. Get Merchant Metadata
    merchant_info = next((m for m in merchant_data_list if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Merchant {target_merchant} not found in merchant_data.json")
        return

    merchant_account_type = merchant_info.get('account_type')
    merchant_mcc = merchant_info.get('merchant_category_code')
    merchant_capture_delay = merchant_info.get('capture_delay')
    
    # 4. Calculate Monthly Stats (Volume & Fraud Rate)
    # Add month column
    df_filtered['month'] = df_filtered['day_of_year'].apply(lambda x: get_month_from_doy(target_year, x))
    
    # Group by month
    monthly_stats = {}
    for month, group in df_filtered.groupby('month'):
        total_vol = group['eur_amount'].sum()
        fraud_vol = group[group['has_fraudulent_dispute']]['eur_amount'].sum()
        fraud_rate = fraud_vol / total_vol if total_vol > 0 else 0.0
        monthly_stats[month] = {
            'volume': total_vol,
            'fraud_rate': fraud_rate
        }

    # 5. Find Applicable Fee IDs
    applicable_fee_ids = set()
    
    # Iterate through transactions (optimized by grouping unique profiles per month)
    # We group by attributes that affect fee rules
    group_cols = ['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country', 'month']
    
    # Check if columns exist
    missing_cols = [c for c in group_cols if c not in df_filtered.columns]
    if missing_cols:
        print(f"Missing columns in payments data: {missing_cols}")
        return

    grouped = df_filtered.groupby(group_cols).size().reset_index(name='count')
    
    for _, row in grouped.iterrows():
        month = row['month']
        stats = monthly_stats.get(month)
        
        # Construct Context
        context = {
            'card_scheme': row['card_scheme'],
            'account_type': merchant_account_type,
            'mcc': merchant_mcc,
            'is_credit': row['is_credit'],
            'aci': row['aci'],
            'intracountry': row['issuing_country'] == row['acquirer_country'],
            'capture_delay': merchant_capture_delay,
            'monthly_volume': stats['volume'],
            'monthly_fraud_rate': stats['fraud_rate']
        }
        
        # Check against all rules
        for rule in fees_data:
            if match_fee_rule(context, rule):
                applicable_fee_ids.add(rule['ID'])

    # 6. Output Result
    sorted_ids = sorted(list(applicable_fee_ids))
    print(", ".join(map(str, sorted_ids)))

if __name__ == "__main__":
    execute_step()
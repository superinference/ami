import pandas as pd
import json
import numpy as np

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
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        # Range handling (e.g., "50-60") - return mean for simple coercion, 
        # but for logic we usually parse ranges separately.
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        try:
            return float(v)
        except ValueError:
            return None
    return None

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '0%-5%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle percentages
    is_percent = '%' in s
    s = s.replace('%', '')
    
    # Handle k/m suffixes
    def parse_val(val_s):
        val_s = val_s.strip()
        mult = 1
        if 'k' in val_s:
            mult = 1000
            val_s = val_s.replace('k', '')
        elif 'm' in val_s:
            mult = 1_000_000
            val_s = val_s.replace('m', '')
        
        try:
            val = float(val_s) * mult
            return val / 100 if is_percent else val
        except:
            return None

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('>'):
        return parse_val(s[1:]), float('inf')
    elif s.startswith('<'):
        return float('-inf'), parse_val(s[1:])
    else:
        # Exact value treated as range [val, val]
        v = parse_val(s)
        return v, v

def get_month_from_doy(doy, year=2023):
    """Returns month (1-12) from day of year."""
    # Days in months for non-leap year (2023)
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cumulative = 0
    for i, days in enumerate(days_in_months):
        cumulative += days
        if doy <= cumulative:
            return i + 1
    return 12

def check_rule_match(tx, rule, merchant_info, monthly_stats):
    """
    Checks if a transaction matches a fee rule.
    
    Args:
        tx: Transaction row (Series/dict)
        rule: Fee rule (dict)
        merchant_info: Merchant metadata (dict)
        monthly_stats: Dict of {month: {'volume': float, 'fraud_rate': float}}
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx['card_scheme']:
        return False

    # 2. Account Type (List contains)
    # Rule account_type: [] or null means ALL. If not empty, must contain merchant's type.
    if rule.get('account_type'):
        if merchant_info.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List contains)
    # Rule mcc: [] or null means ALL. If not empty, must contain merchant's mcc.
    if rule.get('merchant_category_code'):
        if merchant_info.get('merchant_category_code') not in rule['merchant_category_code']:
            return False

    # 4. Capture Delay (Exact match)
    # Rule capture_delay: null means ALL.
    if rule.get('capture_delay'):
        # Handle range logic for capture delay if necessary, but data shows strings like "manual", "immediate", "1"
        # Manual implies strict string matching or specific logic.
        # Let's assume exact match or simple comparison for now based on data samples.
        # If rule has comparison (e.g. >5), we need to parse.
        rule_cd = str(rule['capture_delay'])
        merch_cd = str(merchant_info.get('capture_delay'))
        
        if rule_cd.startswith('>'):
            try:
                limit = float(rule_cd[1:])
                val = float(merch_cd)
                if not (val > limit): return False
            except:
                pass # If conversion fails, ignore or fail? Assuming data is clean enough or string match
        elif rule_cd.startswith('<'):
            try:
                limit = float(rule_cd[1:])
                val = float(merch_cd)
                if not (val < limit): return False
            except:
                pass
        elif '-' in rule_cd:
             # Range e.g. 3-5
             try:
                 low, high = map(float, rule_cd.split('-'))
                 val = float(merch_cd)
                 if not (low <= val <= high): return False
             except:
                 pass
        else:
            if rule_cd != merch_cd:
                return False

    # 5. Is Credit (Boolean)
    # Rule is_credit: null means ALL.
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx['is_credit']:
            return False

    # 6. ACI (List contains)
    # Rule aci: [] or null means ALL.
    if rule.get('aci'):
        if tx['aci'] not in rule['aci']:
            return False

    # 7. Intracountry (Boolean)
    # Rule intracountry: null means ALL.
    # Intracountry = (issuing_country == acquirer_country)
    if rule.get('intracountry') is not None:
        is_intra = (tx['issuing_country'] == tx['acquirer_country'])
        # The rule field might be 0.0/1.0 or boolean.
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 8. Monthly Volume (Range)
    # Rule monthly_volume: null means ALL.
    if rule.get('monthly_volume'):
        month = tx['month_idx']
        vol = monthly_stats.get(month, {}).get('volume', 0)
        min_v, max_v = parse_range(rule['monthly_volume'])
        if min_v is not None:
            if not (min_v <= vol <= max_v):
                return False

    # 9. Monthly Fraud Level (Range)
    # Rule monthly_fraud_level: null means ALL.
    if rule.get('monthly_fraud_level'):
        month = tx['month_idx']
        fraud = monthly_stats.get(month, {}).get('fraud_rate', 0)
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if min_f is not None:
            if not (min_f <= fraud <= max_f):
                return False

    return True

# ═══════════════════════════════════════════════════════════
# MAIN LOGIC
# ═══════════════════════════════════════════════════════════

def main():
    # File paths
    payments_path = '/output/chunk4/data/context/payments.csv'
    fees_path = '/output/chunk4/data/context/fees.json'
    merchant_path = '/output/chunk4/data/context/merchant_data.json'

    # 1. Load Data
    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)

    # 2. Identify Target Merchant and Year
    target_merchant = 'Belles_cookbook_store'
    target_year = 2023
    
    # Get Merchant Metadata
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
        return

    # 3. Identify Target Fee Rule (ID=276)
    target_fee_id = 276
    fee_rule = next((f for f in fees_data if f['ID'] == target_fee_id), None)
    if not fee_rule:
        print(f"Error: Fee ID {target_fee_id} not found in fees.json")
        return

    old_rate = fee_rule['rate']
    new_rate = 99
    
    # 4. Filter Transactions for Merchant and Year
    df_merch = df_payments[
        (df_payments['merchant'] == target_merchant) & 
        (df_payments['year'] == target_year)
    ].copy()
    
    if df_merch.empty:
        print("0.00000000000000")
        return

    # 5. Calculate Monthly Stats (Volume and Fraud Rate)
    # Add month index to dataframe
    df_merch['month_idx'] = df_merch['day_of_year'].apply(lambda x: get_month_from_doy(x, target_year))
    
    monthly_stats = {}
    for month in range(1, 13):
        df_month = df_merch[df_merch['month_idx'] == month]
        if df_month.empty:
            monthly_stats[month] = {'volume': 0.0, 'fraud_rate': 0.0}
            continue
        
        total_vol = df_month['eur_amount'].sum()
        # Fraud rate = Fraudulent Volume / Total Volume (based on Manual Section 7)
        fraud_vol = df_month[df_month['has_fraudulent_dispute'] == True]['eur_amount'].sum()
        
        fraud_rate = (fraud_vol / total_vol) if total_vol > 0 else 0.0
        
        monthly_stats[month] = {
            'volume': total_vol,
            'fraud_rate': fraud_rate
        }

    # 6. Find Matching Transactions
    affected_volume = 0.0
    
    # Iterate through transactions to check applicability
    # Note: We iterate because rules depend on transaction-specific fields (is_credit, aci, etc.)
    # and month-specific stats.
    
    for _, tx in df_merch.iterrows():
        if check_rule_match(tx, fee_rule, merchant_info, monthly_stats):
            affected_volume += tx['eur_amount']

    # 7. Calculate Delta
    # Fee formula: fixed + (rate * amount / 10000)
    # Delta = New_Fee - Old_Fee
    #       = (fixed + new_rate * amt / 10000) - (fixed + old_rate * amt / 10000)
    #       = (new_rate - old_rate) * amt / 10000
    
    delta = (new_rate - old_rate) * affected_volume / 10000
    
    # Print result with high precision
    print(f"{delta:.14f}")

if __name__ == "__main__":
    main()
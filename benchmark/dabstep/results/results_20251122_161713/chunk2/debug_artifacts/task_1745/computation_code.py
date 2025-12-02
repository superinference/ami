import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
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

def parse_range(range_str):
    """Parses a range string like '100k-1m' or '<3' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().replace(',', '').replace('%', '')
    multiplier = 1
    if 'k' in s:
        multiplier = 1000
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000
        s = s.replace('m', '')
        
    if '-' in s:
        try:
            parts = s.split('-')
            return float(parts[0]) * multiplier, float(parts[1]) * multiplier
        except:
            return None, None
    elif '>' in s:
        try:
            val = float(s.replace('>', '')) * multiplier
            return val, float('inf')
        except:
            return None, None
    elif '<' in s:
        try:
            val = float(s.replace('<', '')) * multiplier
            return float('-inf'), val
        except:
            return None, None
    else:
        try:
            val = float(s) * multiplier
            return val, val
        except:
            return None, None

def check_range_match(value, rule_range_str, is_percentage=False):
    """Checks if a value fits within a rule's range string."""
    if rule_range_str is None:
        return True
    
    # Special handling for percentage strings in rules
    if is_percentage and isinstance(rule_range_str, str) and '%' in rule_range_str:
        clean_range = rule_range_str.replace('%', '')
        min_v, max_v = parse_range(clean_range)
        if min_v is not None:
            # parse_range returns raw numbers (e.g. 7, 8 for 7-8), divide by 100 for percentage comparison
            return (min_v/100) <= value <= (max_v/100)
            
    # Standard handling
    min_v, max_v = parse_range(rule_range_str)
    if min_v is None:
        return False 
    return min_v <= value <= max_v

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_ctx: dict containing transaction/merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False

    # 2. Account Type (List match)
    if rule.get('account_type'):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_ctx.get('mcc') not in rule['merchant_category_code']:
            return False

    # 4. Capture Delay (Exact match or Range)
    if rule.get('capture_delay'):
        rule_cd = rule['capture_delay']
        tx_cd = tx_ctx.get('capture_delay')
        
        if rule_cd == tx_cd:
            pass
        elif tx_cd and tx_cd.isdigit():
            days = int(tx_cd)
            if rule_cd == '<3' and days < 3: pass
            elif rule_cd == '>5' and days > 5: pass
            elif rule_cd == '3-5' and 3 <= days <= 5: pass
            else: return False
        else:
            return False

    # 5. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False

    # 6. ACI (List match)
    if rule.get('aci'):
        if tx_ctx.get('aci') not in rule['aci']:
            return False

    # 7. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Assuming 1.0/0.0 in JSON maps to True/False
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_ctx.get('intracountry'):
            return False

    # 8. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not check_range_match(tx_ctx.get('monthly_volume', 0), rule['monthly_volume']):
            return False

    # 9. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not check_range_match(tx_ctx.get('monthly_fraud_level', 0), rule['monthly_fraud_level'], is_percentage=True):
            return False

    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# Load Data
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'

try:
    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)

    # 1. Filter for Rafa_AI
    merchant_name = 'Rafa_AI'
    df_rafa = df_payments[df_payments['merchant'] == merchant_name].copy()

    if df_rafa.empty:
        print("No transactions found for Rafa_AI")
    else:
        # 2. Get Static Merchant Attributes
        merchant_info = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
        if not merchant_info:
            print(f"Merchant {merchant_name} not found in merchant_data.json")
            exit()

        static_ctx = {
            'account_type': merchant_info.get('account_type'),
            'mcc': merchant_info.get('merchant_category_code'),
            'capture_delay': merchant_info.get('capture_delay')
        }

        # 3. Calculate Monthly Stats (Volume and Fraud)
        # Convert day_of_year to month (2023)
        df_rafa['date'] = pd.to_datetime(df_rafa['year'] * 1000 + df_rafa['day_of_year'], format='%Y%j')
        df_rafa['month'] = df_rafa['date'].dt.month

        # Group by month to calculate stats
        monthly_stats = df_rafa.groupby('month').agg(
            total_vol=('eur_amount', 'sum'),
            fraud_vol=('eur_amount', lambda x: x[df_rafa.loc[x.index, 'has_fraudulent_dispute']].sum())
        ).reset_index()

        monthly_stats['fraud_rate'] = monthly_stats['fraud_vol'] / monthly_stats['total_vol']
        
        # Create a lookup for monthly stats: month -> {vol, fraud_rate}
        stats_lookup = monthly_stats.set_index('month').to_dict('index')

        # 4. Identify Unique Transaction Profiles
        # Group by attributes that affect fees to optimize matching
        profile_cols = ['card_scheme', 'is_credit', 'aci', 'issuing_country', 'acquirer_country', 'month']
        unique_profiles = df_rafa[profile_cols].drop_duplicates()

        applicable_fee_ids = set()

        # 5. Match Rules
        for _, row in unique_profiles.iterrows():
            month = row['month']
            stats = stats_lookup.get(month, {'total_vol': 0, 'fraud_rate': 0})
            
            # Build Context
            ctx = {
                'card_scheme': row['card_scheme'],
                'is_credit': row['is_credit'],
                'aci': row['aci'],
                'intracountry': row['issuing_country'] == row['acquirer_country'],
                'monthly_volume': stats['total_vol'],
                'monthly_fraud_level': stats['fraud_rate'],
                **static_ctx # Add static attributes
            }
            
            # Check all rules
            for rule in fees_data:
                if match_fee_rule(ctx, rule):
                    applicable_fee_ids.add(rule['ID'])

        # 6. Output
        sorted_ids = sorted(list(applicable_fee_ids))
        print(f"Applicable Fee IDs for {merchant_name} in 2023:")
        print(", ".join(map(str, sorted_ids)))

except Exception as e:
    print(f"An error occurred: {e}")
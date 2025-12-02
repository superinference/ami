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
        return float(v)
    return float(value)

def parse_range(range_str):
    """Parses strings like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if range_str is None or range_str == 'None':
        return None, None
    
    s = str(range_str).lower().strip()
    
    # Helper to parse value with suffixes
    def parse_val(x):
        x = x.strip()
        mult = 1
        if x.endswith('%'):
            x = x[:-1]
            mult = 0.01
        elif x.endswith('k'):
            x = x[:-1]
            mult = 1000
        elif x.endswith('m'):
            x = x[:-1]
            mult = 1000000
        return float(x) * mult

    try:
        if '-' in s:
            parts = s.split('-')
            return parse_val(parts[0]), parse_val(parts[1])
        elif s.startswith('>'):
            val = parse_val(s[1:])
            return val, float('inf')
        elif s.startswith('<'):
            val = parse_val(s[1:])
            return float('-inf'), val
        else:
            # Exact value treated as range [val, val]
            val = parse_val(s)
            return val, val
    except:
        return None, None

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    Returns True if the rule applies, False otherwise.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. is_credit (Boolean match or None/Null)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 3. Intracountry (Boolean match or None/Null)
    # JSON might load 0.0/1.0 for False/True
    rule_intra = rule.get('intracountry')
    if rule_intra is not None:
        # Normalize to boolean
        if rule_intra == 0.0: rule_intra = False
        elif rule_intra == 1.0: rule_intra = True
        
        if bool(rule_intra) != tx_ctx['intracountry']:
            return False

    # 4. ACI (List match or Empty/None)
    # If rule['aci'] is present and not empty, tx_ctx['aci'] must be in it
    if rule.get('aci'): 
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 5. Account Type (List match or Empty/None)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 6. MCC (List match or Empty/None)
    if rule.get('merchant_category_code'):
        if tx_ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False

    # 7. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if min_v is not None:
            vol = tx_ctx.get('monthly_volume', 0)
            if not (min_v <= vol <= max_v):
                return False

    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        if min_f is not None:
            fraud = tx_ctx.get('monthly_fraud_level', 0)
            # Use small epsilon for float comparison if needed, or direct comparison
            # Note: fraud is a ratio (0.08), range is usually parsed to ratio (0.08)
            if not (min_f <= fraud <= max_f + 1e-9): 
                return False
                
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0) or 0
    rate = rule.get('rate', 0) or 0
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000)

# --- Main Execution ---
def execute_step():
    print("Starting analysis...")
    
    # 1. Load Data
    try:
        # Load full payments for stats calculation
        df_all = pd.read_csv('/output/chunk2/data/context/payments.csv')
        print(f"Loaded payments.csv: {len(df_all)} rows")
        
        with open('/output/chunk2/data/context/fees.json', 'r') as f:
            fees = json.load(f)
        print(f"Loaded fees.json: {len(fees)} rules")
            
        with open('/output/chunk2/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
        print(f"Loaded merchant_data.json: {len(merchant_data)} merchants")
            
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # 2. Prepare Merchant Data Lookup
    # Create a dict for faster lookup: merchant_name -> {mcc, account_type}
    merchant_lookup = {}
    for m in merchant_data:
        merchant_lookup[m['merchant']] = {
            'merchant_category_code': m['merchant_category_code'],
            'account_type': m['account_type']
        }

    # 3. Calculate Monthly Stats for Merchants (Volume & Fraud)
    # Add month column (2023 is not a leap year)
    df_all['date'] = pd.to_datetime(df_all['year'] * 1000 + df_all['day_of_year'], format='%Y%j')
    df_all['month'] = df_all['date'].dt.month
    
    # Group by merchant and month to calculate total volume and fraud volume
    monthly_stats = df_all.groupby(['merchant', 'month']).apply(
        lambda x: pd.Series({
            'total_vol': x['eur_amount'].sum(),
            'fraud_vol': x[x['has_fraudulent_dispute']]['eur_amount'].sum()
        })
    ).reset_index()
    
    # Calculate fraud ratio (Fraud Volume / Total Volume)
    monthly_stats['fraud_ratio'] = monthly_stats['fraud_vol'] / monthly_stats['total_vol']
    
    # Create a lookup for stats: (merchant, month) -> {vol, fraud_ratio}
    stats_lookup = {}
    for _, row in monthly_stats.iterrows():
        stats_lookup[(row['merchant'], row['month'])] = {
            'monthly_volume': row['total_vol'],
            'monthly_fraud_level': row['fraud_ratio']
        }

    # 4. Filter Target Transactions (NexPay + Credit)
    df_nexpay_credit = df_all[
        (df_all['card_scheme'] == 'NexPay') & 
        (df_all['is_credit'] == True)
    ].copy()
    print(f"Target transactions (NexPay Credit): {len(df_nexpay_credit)} rows")

    # 5. Filter Fee Rules for NexPay Credit
    # Optimization: Pre-filter fees to reduce iteration count
    relevant_fees = [
        f for f in fees 
        if f['card_scheme'] == 'NexPay' 
        and (f['is_credit'] is True or f['is_credit'] is None)
    ]
    print(f"Relevant fee rules found: {len(relevant_fees)}")

    # 6. Iterate Transactions and Calculate Fees
    calculated_fees = []
    
    # Ensure month is present in target df (it is, from df_all)
    
    for _, tx in df_nexpay_credit.iterrows():
        merchant = tx['merchant']
        month = tx['month']
        
        # Get Merchant Attributes
        m_attrs = merchant_lookup.get(merchant)
        if not m_attrs:
            continue 
            
        # Get Monthly Stats
        stats = stats_lookup.get((merchant, month))
        if not stats:
            stats = {'monthly_volume': 0, 'monthly_fraud_level': 0}
            
        # Determine Intracountry
        # "True if the transaction is domestic... issuer country and the acquiring country are the same"
        is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
        
        # Build Context for Matching
        tx_ctx = {
            'card_scheme': 'NexPay',
            'is_credit': True,
            'aci': tx['aci'],
            'account_type': m_attrs['account_type'],
            'merchant_category_code': m_attrs['merchant_category_code'],
            'intracountry': is_intracountry,
            'monthly_volume': stats['monthly_volume'],
            'monthly_fraud_level': stats['monthly_fraud_level']
        }
        
        # Find First Matching Rule
        matched_rule = None
        for rule in relevant_fees:
            if match_fee_rule(tx_ctx, rule):
                matched_rule = rule
                break 
        
        if matched_rule:
            # Calculate fee for 10 EUR transaction value
            fee = calculate_fee(10.0, matched_rule)
            calculated_fees.append(fee)

    # 7. Compute Average
    if calculated_fees:
        avg_fee = sum(calculated_fees) / len(calculated_fees)
        print(f"\nCalculated fees for {len(calculated_fees)} transactions.")
        print(f"Average fee for 10 EUR transaction: {avg_fee:.14f}")
    else:
        print("No applicable fees found.")

if __name__ == "__main__":
    execute_step()
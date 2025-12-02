import pandas as pd
import json
import numpy as np

# ---------------------------------------------------------
# HELPER FUNCTIONS (Robust Data Processing)
# ---------------------------------------------------------

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
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value) if value is not None else 0.0

def parse_range(range_str):
    """Parses a range string like '100k-1m', '>50', '0%-1%' into (min, max)."""
    if range_str is None:
        return -float('inf'), float('inf')
    
    s = str(range_str).lower().strip()
    
    # Handle units
    multiplier = 1
    if 'k' in s:
        multiplier = 1000
        s = s.replace('k', '')
    elif 'm' in s:
        multiplier = 1000000
        s = s.replace('m', '')
        
    is_percent = '%' in s
    if is_percent:
        s = s.replace('%', '')
        
    try:
        if '-' in s:
            parts = s.split('-')
            low = float(parts[0])
            high = float(parts[1])
        elif '>' in s:
            low = float(s.replace('>', ''))
            high = float('inf')
        elif '<' in s:
            low = -float('inf')
            high = float(s.replace('<', ''))
        else:
            # Exact value or malformed
            val = float(s)
            low = val
            high = val
            
        if is_percent:
            low /= 100
            high /= 100
        else:
            low *= multiplier
            high *= multiplier
            
        return low, high
    except:
        return -float('inf'), float('inf')

def match_fee_rule(ctx, rule):
    """
    Checks if a transaction context matches a fee rule.
    ctx: dict containing transaction details and merchant stats
    rule: dict containing fee rule criteria
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List containment)
    # Rule has list of allowed types. Merchant has one type.
    # If rule list is empty/null, it applies to all.
    if rule.get('account_type'):
        if ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List containment)
    if rule.get('merchant_category_code'):
        if ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Exact match or wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False
            
    # 5. ACI (List containment)
    if rule.get('aci'):
        if ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Exact match or wildcard)
    if rule.get('intracountry') is not None:
        # Intracountry is True if issuing_country == acquirer_country
        is_intra = (ctx['issuing_country'] == ctx['acquirer_country'])
        # Compare boolean values. Note: rule['intracountry'] might be 0.0/1.0 from JSON
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False

    # 7. Monthly Volume (Range check)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= ctx['monthly_volume'] <= max_v):
            return False
            
    # 8. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        # Fraud level is a ratio (0.0 to 1.0)
        if not (min_f <= ctx['monthly_fraud_level'] <= max_f):
            return False
            
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = rule.get('fixed_amount', 0.0) or 0.0
    rate = rule.get('rate', 0) or 0
    # Rate is an integer to be divided by 10000
    variable = (rate * amount) / 10000
    return fixed + variable

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# File paths
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchants_data = json.load(f)

# 2. Get Merchant Attributes
target_merchant = 'Golfclub_Baron_Friso'
merchant_info = next((m for m in merchants_data if m['merchant'] == target_merchant), None)

if not merchant_info:
    print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
else:
    merchant_account_type = merchant_info['account_type']
    merchant_mcc = merchant_info['merchant_category_code']
    
    # 3. Calculate Monthly Stats (January 2023)
    # Day 12 is in January. We need stats for the full month to determine fee tier.
    # Filter for Merchant + Year 2023 + Days 1-31 (January)
    jan_mask = (
        (df_payments['merchant'] == target_merchant) & 
        (df_payments['year'] == 2023) & 
        (df_payments['day_of_year'] >= 1) & 
        (df_payments['day_of_year'] <= 31)
    )
    df_jan = df_payments[jan_mask]
    
    monthly_volume = df_jan['eur_amount'].sum()
    
    # Fraud Level: Ratio of fraudulent volume over total volume (per manual.md)
    fraud_volume = df_jan[df_jan['has_fraudulent_dispute']]['eur_amount'].sum()
    monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0
    
    print(f"Merchant: {target_merchant}")
    print(f"Jan 2023 Volume: €{monthly_volume:,.2f}")
    print(f"Jan 2023 Fraud Level: {monthly_fraud_level:.4%}")

    # 4. Filter Transactions for Specific Day (Day 12)
    day_mask = (
        (df_payments['merchant'] == target_merchant) & 
        (df_payments['year'] == 2023) & 
        (df_payments['day_of_year'] == 12)
    )
    df_day = df_payments[day_mask]
    
    print(f"Transactions on Day 12: {len(df_day)}")
    
    # 5. Calculate Fees
    total_fees = 0.0
    
    for _, tx in df_day.iterrows():
        # Build context for this transaction
        ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': merchant_account_type,
            'mcc': merchant_mcc,
            'is_credit': bool(tx['is_credit']),
            'aci': tx['aci'],
            'issuing_country': tx['issuing_country'],
            'acquirer_country': tx['acquirer_country'],
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_level
        }
        
        # Find matching rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break # Assume first match wins
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_fees += fee
        else:
            print(f"Warning: No fee rule found for tx {tx['psp_reference']}")
            
    print(f"\nTotal fees for {target_merchant} on Day 12, 2023: {total_fees:.2f}")
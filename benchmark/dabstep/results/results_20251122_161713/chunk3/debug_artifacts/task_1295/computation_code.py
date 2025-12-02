import pandas as pd
import json
import numpy as np
from datetime import datetime, timedelta

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None:
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

def parse_range_check(value, range_str):
    """
    Checks if a numeric value fits within a range string (e.g., '100k-1m', '>5', '<3%').
    """
    if range_str is None:
        return True
    
    # Handle percentages in range string
    is_percent = '%' in range_str
    clean_range = range_str.replace('%', '').replace(',', '')
    
    # Handle k/m suffixes for volume
    if 'k' in clean_range.lower() or 'm' in clean_range.lower():
        def parse_suffix(s):
            s = s.lower().strip()
            if 'k' in s: return float(s.replace('k', '')) * 1000
            if 'm' in s: return float(s.replace('m', '')) * 1000000
            return float(s)
    else:
        def parse_suffix(s): return float(s)

    try:
        if '-' in clean_range:
            low, high = clean_range.split('-')
            low_val = parse_suffix(low)
            high_val = parse_suffix(high)
            if is_percent:
                low_val /= 100
                high_val /= 100
            return low_val <= value <= high_val
        
        if clean_range.startswith('>'):
            limit = parse_suffix(clean_range[1:])
            if is_percent: limit /= 100
            return value > limit
            
        if clean_range.startswith('<'):
            limit = parse_suffix(clean_range[1:])
            if is_percent: limit /= 100
            return value < limit
            
        # Exact match (rare for ranges but possible)
        val_check = parse_suffix(clean_range)
        if is_percent: val_check /= 100
        return value == val_check
        
    except Exception:
        return False

def match_fee_rule(tx_ctx, rule):
    """
    Determines if a fee rule applies to a specific transaction context.
    tx_ctx: dict containing transaction details (merchant stats, tx props)
    rule: dict from fees.json
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx.get('card_scheme'):
        return False
        
    # 2. Is Credit (Boolean or None/Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx.get('is_credit'):
            return False
            
    # 3. Merchant Category Code (List of ints)
    if rule.get('merchant_category_code'):
        if tx_ctx.get('mcc') not in rule['merchant_category_code']:
            return False
            
    # 4. Account Type (List of strings)
    if rule.get('account_type'):
        if tx_ctx.get('account_type') not in rule['account_type']:
            return False
            
    # 5. ACI (List of strings)
    if rule.get('aci'):
        if tx_ctx.get('aci') not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean or None)
    if rule.get('intracountry') is not None:
        # Convert 0.0/1.0 to bool if necessary
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx.get('intracountry'):
            return False
            
    # 7. Monthly Volume (Range string)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_ctx.get('monthly_volume', 0), rule['monthly_volume']):
            return False
            
    # 8. Monthly Fraud Level (Range string)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_ctx.get('monthly_fraud_level', 0), rule['monthly_fraud_level']):
            return False
            
    # 9. Capture Delay (String match or range)
    if rule.get('capture_delay'):
        # Simple string match for now as per data samples (e.g., 'manual', 'immediate')
        # If rule has range like '>5', we need to parse. 
        # Merchant data has 'manual', 'immediate', '1', '7'.
        rule_cd = rule['capture_delay']
        merch_cd = str(tx_ctx.get('capture_delay', ''))
        
        if rule_cd.startswith('>'):
            try:
                limit = float(rule_cd[1:])
                val = float(merch_cd) if merch_cd.replace('.','').isdigit() else 0
                if val <= limit: return False
            except:
                if merch_cd not in ['manual']: # manual is usually long
                    return False
        elif rule_cd.startswith('<'):
            try:
                limit = float(rule_cd[1:])
                val = float(merch_cd) if merch_cd.replace('.','').isdigit() else 0
                if val >= limit: return False
            except:
                pass
        elif rule_cd != merch_cd:
            return False

    return True

# ---------------------------------------------------------
# MAIN SCRIPT
# ---------------------------------------------------------

# Define file paths
payments_path = '/output/chunk3/data/context/payments.csv'
fees_path = '/output/chunk3/data/context/fees.json'
merchant_data_path = '/output/chunk3/data/context/merchant_data.json'

# 1. Load Data
print("Loading data...")
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data_list = json.load(f)

# Convert merchant data to dict for fast lookup
merchant_lookup = {m['merchant']: m for m in merchant_data_list}

# 2. Pre-calculate Monthly Stats (Volume and Fraud)
# Convert day_of_year to month
# 2023 is not a leap year
def get_month(day_of_year):
    date = datetime(2023, 1, 1) + timedelta(days=int(day_of_year) - 1)
    return date.month

df_payments['month'] = df_payments['day_of_year'].apply(get_month)

# Calculate stats per merchant per month
print("Calculating monthly stats...")
monthly_stats = df_payments.groupby(['merchant', 'month']).agg(
    total_volume=('eur_amount', 'sum'),
    fraud_count=('has_fraudulent_dispute', 'sum'),
    tx_count=('psp_reference', 'count')
).reset_index()

monthly_stats['fraud_rate'] = monthly_stats['fraud_count'] / monthly_stats['tx_count']

# Create a lookup for stats: (merchant, month) -> {vol, fraud}
stats_lookup = {}
for _, row in monthly_stats.iterrows():
    stats_lookup[(row['merchant'], row['month'])] = {
        'volume': row['total_volume'],
        'fraud_rate': row['fraud_rate']
    }

# 3. Filter Target Transactions
# Question: "For credit transactions, what would be the average fee... SwiftCharge... 5000 EUR?"
# We filter for SwiftCharge + Credit to get the distribution of transaction contexts (ACI, Merchant, Country, etc.)
print("Filtering SwiftCharge Credit transactions...")
df_target = df_payments[
    (df_payments['card_scheme'] == 'SwiftCharge') & 
    (df_payments['is_credit'] == True)
].copy()

print(f"Found {len(df_target)} matching transactions.")

# 4. Calculate Fees
fees_calculated = []
target_amount = 5000.0

for idx, row in df_target.iterrows():
    merchant_name = row['merchant']
    month = row['month']
    
    # Get Merchant Metadata
    m_data = merchant_lookup.get(merchant_name)
    if not m_data:
        continue
        
    # Get Monthly Stats
    stats = stats_lookup.get((merchant_name, month))
    if not stats:
        continue
        
    # Determine Intracountry
    is_intracountry = (row['issuing_country'] == row['acquirer_country'])
    
    # Build Context
    tx_context = {
        'card_scheme': 'SwiftCharge',
        'is_credit': True,
        'mcc': m_data['merchant_category_code'],
        'account_type': m_data['account_type'],
        'aci': row['aci'],
        'intracountry': is_intracountry,
        'monthly_volume': stats['volume'],
        'monthly_fraud_level': stats['fraud_rate'],
        'capture_delay': m_data['capture_delay']
    }
    
    # Find Matching Rule
    matched_rule = None
    # Iterate through fees (assuming order matters or first match is sufficient)
    # In many fee structures, specific rules override generic ones. 
    # However, without explicit priority logic, we scan for the first valid match.
    # If multiple match, usually the most specific one applies, but here we'll take the first found
    # that satisfies all non-null constraints.
    
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Calculate Fee for 5000 EUR
        # Fee = Fixed + (Rate * Amount / 10000)
        fixed = matched_rule['fixed_amount']
        rate = matched_rule['rate']
        fee = fixed + (rate * target_amount / 10000)
        fees_calculated.append(fee)

# 5. Compute Average
if fees_calculated:
    average_fee = sum(fees_calculated) / len(fees_calculated)
    print(f"Average fee for 5000 EUR transaction: {average_fee:.14f}")
else:
    print("No applicable fee rules found for the filtered transactions.")
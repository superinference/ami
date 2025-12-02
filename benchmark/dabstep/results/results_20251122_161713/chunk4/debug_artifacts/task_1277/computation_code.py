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

def parse_range_check(value, rule_range_str):
    """Checks if a numeric value falls within a string range (e.g., '100k-1m', '>5')."""
    if rule_range_str is None:
        return True
    
    # Handle K/M suffixes
    def parse_val(s):
        s = s.lower().strip().replace('%', '')
        mult = 1
        if 'k' in s:
            mult = 1000
            s = s.replace('k', '')
        elif 'm' in s:
            mult = 1_000_000
            s = s.replace('m', '')
        return float(s) * mult

    try:
        if '-' in rule_range_str:
            low, high = rule_range_str.split('-')
            # Handle percentages in ranges
            is_percent = '%' in rule_range_str
            l = parse_val(low)
            h = parse_val(high)
            if is_percent:
                l /= 100
                h /= 100
            return l <= value <= h
        elif '>' in rule_range_str:
            limit = parse_val(rule_range_str.replace('>', ''))
            if '%' in rule_range_str: limit /= 100
            return value > limit
        elif '<' in rule_range_str:
            limit = parse_val(rule_range_str.replace('<', ''))
            if '%' in rule_range_str: limit /= 100
            return value < limit
        else:
            # Exact match (unlikely for ranges but possible)
            return value == parse_val(rule_range_str)
    except:
        return False

def check_capture_delay(merchant_delay, rule_delay):
    """Matches merchant capture delay string to rule requirement."""
    if rule_delay is None:
        return True
    
    # Map numeric strings to integers for comparison
    try:
        delay_days = int(merchant_delay)
    except ValueError:
        delay_days = None # 'immediate' or 'manual'

    if rule_delay == 'immediate':
        return merchant_delay == 'immediate'
    elif rule_delay == 'manual':
        return merchant_delay == 'manual'
    elif rule_delay == '<3':
        return merchant_delay == 'immediate' or (delay_days is not None and delay_days < 3)
    elif rule_delay == '>5':
        return merchant_delay == 'manual' or (delay_days is not None and delay_days > 5)
    elif '-' in rule_delay: # e.g. '3-5'
        low, high = map(int, rule_delay.split('-'))
        return delay_days is not None and low <= delay_days <= high
    
    return False

def get_month(day_of_year):
    """Returns month (1-12) from day_of_year (1-365) for non-leap year."""
    # Cumulative days at start of each month
    starts = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    for i, start in enumerate(starts):
        if day_of_year <= start:
            return i # Previous month index is the month number (1-based adjustment happens naturally)
    return 12 # December

def get_month_from_doy(doy):
    """Vectorized or scalar month lookup."""
    # Simple approximation or exact lookup
    months = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 366]
    for i in range(12):
        if doy <= months[i+1]:
            return i + 1
    return 12

def match_fee_rule(ctx, rule):
    """
    Matches a transaction context against a fee rule.
    ctx: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Is Credit (Boolean or None)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False
            
    # 3. Merchant Category Code (List or None)
    if rule.get('merchant_category_code'):
        if ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Account Type (List or None)
    if rule.get('account_type'):
        if ctx['account_type'] not in rule['account_type']:
            return False
            
    # 5. ACI (List or None)
    if rule.get('aci'):
        if ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean or None)
    if rule.get('intracountry') is not None:
        if rule['intracountry'] != ctx['intracountry']:
            return False
            
    # 7. Capture Delay (String/Range or None)
    if not check_capture_delay(ctx['capture_delay'], rule.get('capture_delay')):
        return False
        
    # 8. Monthly Volume (Range or None)
    if not parse_range_check(ctx['monthly_volume'], rule.get('monthly_volume')):
        return False
        
    # 9. Monthly Fraud Level (Range or None)
    if not parse_range_check(ctx['monthly_fraud_rate'], rule.get('monthly_fraud_level')):
        return False
        
    return True

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------

# 1. Load Data
base_path = '/output/chunk4/data/context/'
df_payments = pd.read_csv(base_path + 'payments.csv')
with open(base_path + 'fees.json', 'r') as f:
    fees_data = json.load(f)
with open(base_path + 'merchant_data.json', 'r') as f:
    merchant_data_list = json.load(f)

# Convert merchant data to dict for fast lookup
merchant_lookup = {m['merchant']: m for m in merchant_data_list}

# 2. Pre-calculate Monthly Stats per Merchant
# Add month column
df_payments['month'] = df_payments['day_of_year'].apply(get_month_from_doy)

# Group by Merchant and Month
monthly_stats = df_payments.groupby(['merchant', 'month']).agg(
    total_volume=('eur_amount', 'sum'),
    fraud_volume=('eur_amount', lambda x: x[df_payments.loc[x.index, 'has_fraudulent_dispute']].sum())
).reset_index()

# Calculate fraud rate (volume based)
monthly_stats['fraud_rate'] = monthly_stats['fraud_volume'] / monthly_stats['total_volume']
# Handle division by zero if any (unlikely given data)
monthly_stats['fraud_rate'] = monthly_stats['fraud_rate'].fillna(0.0)

# Create a lookup dictionary: stats_lookup[merchant][month] = {'vol': ..., 'fraud': ...}
stats_lookup = {}
for _, row in monthly_stats.iterrows():
    if row['merchant'] not in stats_lookup:
        stats_lookup[row['merchant']] = {}
    stats_lookup[row['merchant']][row['month']] = {
        'volume': row['total_volume'],
        'fraud_rate': row['fraud_rate']
    }

# 3. Filter Target Transactions
# Question asks for "GlobalCard" and "credit transactions"
target_txs = df_payments[
    (df_payments['card_scheme'] == 'GlobalCard') & 
    (df_payments['is_credit'] == True)
].copy()

print(f"Processing {len(target_txs)} GlobalCard credit transactions...")

# 4. Calculate Fees for 50 EUR
calculated_fees = []
target_amount = 50.0

# Iterate through each transaction to find the applicable fee
for _, tx in target_txs.iterrows():
    merchant = tx['merchant']
    month = tx['month']
    
    # Get Merchant Context
    m_static = merchant_lookup.get(merchant)
    m_stats = stats_lookup.get(merchant, {}).get(month)
    
    if not m_static or not m_stats:
        continue # Should not happen with consistent data
        
    # Build Context for Matching
    ctx = {
        'card_scheme': tx['card_scheme'],
        'is_credit': tx['is_credit'],
        'mcc': m_static['merchant_category_code'],
        'account_type': m_static['account_type'],
        'capture_delay': m_static['capture_delay'],
        'aci': tx['aci'],
        'intracountry': tx['issuing_country'] == tx['acquirer_country'],
        'monthly_volume': m_stats['volume'],
        'monthly_fraud_rate': m_stats['fraud_rate']
    }
    
    # Find Matching Rule
    # We iterate through fees and pick the first match (assuming priority or specificity is handled by order or uniqueness)
    # In reality, fee structures often have specific precedence, but here we look for the valid rule.
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(ctx, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        # Calculate Fee: fixed + rate * amount / 10000
        # Rate is in basis points (per 10,000) usually, or specified as integer to be divided.
        # Manual says: "rate: integer... multiplied by the transaction value and divided by 10000"
        fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * target_amount / 10000)
        calculated_fees.append(fee)

# 5. Calculate Average
if calculated_fees:
    average_fee = sum(calculated_fees) / len(calculated_fees)
    print(f"Average fee for 50 EUR GlobalCard credit transaction: {average_fee:.4f} EUR")
else:
    print("No matching fees found.")
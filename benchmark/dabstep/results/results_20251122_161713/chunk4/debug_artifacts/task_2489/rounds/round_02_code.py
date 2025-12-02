# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2489
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8020 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        return float(v)
    return float(value)

def parse_range(value_str):
    """Parses strings like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if not isinstance(value_str, str):
        return None, None
    
    s = value_str.strip().lower().replace(',', '').replace('%', '')
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
            low = float(parts[0]) * multiplier
            high = float(parts[1]) * multiplier
            # Handle percentage conversion if original had %
            if '%' in value_str:
                low /= 100
                high /= 100
            return low, high
        except:
            return None, None
    elif '>' in s:
        try:
            val = float(s.replace('>', '')) * multiplier
            if '%' in value_str: val /= 100
            return val, float('inf')
        except:
            return None, None
    elif '<' in s:
        try:
            val = float(s.replace('<', '')) * multiplier
            if '%' in value_str: val /= 100
            return float('-inf'), val
        except:
            return None, None
    return None, None

def match_fee_rule(tx, rule):
    """
    Determines if a fee rule applies to a transaction based on all criteria.
    Handles wildcards (None/Empty) and specific matching logic.
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != tx.get('card_scheme'):
        return False
        
    # 2. Account Type (List in rule)
    if rule.get('account_type'):
        if tx.get('account_type') not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List in rule)
    if rule.get('merchant_category_code'):
        # Ensure types match (int vs str)
        rule_mccs = [int(x) for x in rule['merchant_category_code']]
        if int(tx.get('mcc')) not in rule_mccs:
            return False
            
    # 4. ACI (List in rule)
    if rule.get('aci'):
        if tx.get('aci') not in rule['aci']:
            return False
            
    # 5. Is Credit (Bool)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx.get('is_credit'):
            return False
            
    # 6. Intracountry (Bool)
    if rule.get('intracountry') is not None:
        # Intracountry logic: issuer == acquirer
        is_intra = (tx.get('issuing_country') == tx.get('acquirer_country'))
        rule_intra = bool(rule['intracountry'])
        if rule_intra != is_intra:
            return False
            
    # 7. Capture Delay (String)
    if rule.get('capture_delay'):
        r_cd = str(rule['capture_delay'])
        t_cd = str(tx.get('capture_delay'))
        # Simple match for specific values like 'manual', 'immediate'
        if r_cd != t_cd:
            # If rule is a range/inequality, we would parse it here, 
            # but for this dataset, exact match on 'manual'/'immediate' is primary.
            return False

    # 8. Monthly Volume (Range)
    if rule.get('monthly_volume'):
        low, high = parse_range(rule['monthly_volume'])
        vol = tx.get('monthly_volume_eur', 0)
        if low is not None and (vol < low or vol > high):
            return False

    # 9. Monthly Fraud Level (Range)
    if rule.get('monthly_fraud_level'):
        low, high = parse_range(rule['monthly_fraud_level'])
        fraud = tx.get('monthly_fraud_rate', 0) # 0.0 to 1.0
        if low is not None and (fraud < low or fraud > high):
            return False
            
    return True

def get_month_from_doy(day_of_year):
    """Returns month (1-12) from day of year (1-365)."""
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

# --- Main Execution ---

# File Paths
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_data_path = '/output/chunk4/data/context/merchant_data.json'

# Load Data
try:
    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_data_path, 'r') as f:
        merchant_data = json.load(f)
except Exception as e:
    print(f"Error loading data: {e}")
    exit()

# 1. Filter for Merchant and Year
merchant_name = 'Crossfit_Hanna'
df = df_payments[(df_payments['merchant'] == merchant_name) & (df_payments['year'] == 2023)].copy()

if df.empty:
    print("No transactions found for Crossfit_Hanna in 2023")
    exit()

# 2. Get Merchant Metadata (Account Type, MCC, Capture Delay)
m_info = next((item for item in merchant_data if item["merchant"] == merchant_name), None)
if not m_info:
    print(f"Merchant data not found for {merchant_name}")
    exit()

m_account_type = m_info.get('account_type')
m_mcc = m_info.get('merchant_category_code')
m_capture_delay = m_info.get('capture_delay')

# 3. Calculate Monthly Stats (Volume & Fraud)
# Map day_of_year to month to calculate monthly totals required by fee rules
df['month'] = df['day_of_year'].apply(get_month_from_doy)

monthly_stats = df.groupby('month').agg(
    total_vol=('eur_amount', 'sum'),
    fraud_count=('has_fraudulent_dispute', 'sum'),
    tx_count=('psp_reference', 'count')
).reset_index()

monthly_stats['fraud_rate'] = monthly_stats['fraud_count'] / monthly_stats['tx_count']

# Merge stats back to transactions so each row has its month's context
df = df.merge(monthly_stats[['month', 'total_vol', 'fraud_rate']], on='month', how='left')

# 4. Identify Fee Rule 792
rule_792 = next((r for r in fees_data if r['ID'] == 792), None)
if not rule_792:
    print("Rule 792 not found")
    exit()

# 5. Calculate Delta
# Delta = (New Rate - Old Rate) * Amount / 10000
# New Rate = 1
old_rate = rule_792['rate']
new_rate = 1
rate_diff = new_rate - old_rate

total_delta = 0.0
matching_count = 0

for _, tx in df.iterrows():
    # Construct transaction dictionary with all fields needed for matching
    tx_dict = {
        'card_scheme': tx['card_scheme'],
        'account_type': m_account_type,
        'mcc': m_mcc,
        'aci': tx['aci'],
        'is_credit': tx['is_credit'],
        'issuing_country': tx['issuing_country'],
        'acquirer_country': tx['acquirer_country'],
        'capture_delay': m_capture_delay,
        'monthly_volume_eur': tx['total_vol'],
        'monthly_fraud_rate': tx['fraud_rate']
    }
    
    # Check if Rule 792 applies to this transaction
    if match_fee_rule(tx_dict, rule_792):
        matching_count += 1
        amount = tx['eur_amount']
        # Calculate delta for this transaction
        # Fee formula: fixed + rate * amount / 10000
        # Fixed amount cancels out in delta.
        delta = (rate_diff * amount) / 10000.0
        total_delta += delta

# Output result with high precision as required for delta questions
print(f"{total_delta:.14f}")

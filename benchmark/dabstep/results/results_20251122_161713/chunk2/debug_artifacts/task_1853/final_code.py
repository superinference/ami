import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════

def coerce_to_float(value):
    """Convert string with %, $, commas, k, m to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        
        # Handle k/m suffixes
        multiplier = 1
        if v.lower().endswith('k'):
            multiplier = 1000
            v = v[:-1]
        elif v.lower().endswith('m'):
            multiplier = 1000000
            v = v[:-1]
            
        if '%' in v:
            return (float(v.replace('%', '')) / 100) * multiplier
            
        try:
            return float(v) * multiplier
        except ValueError:
            return 0.0
    return 0.0

def parse_range(range_str):
    """Parses a range string like '100k-1m', '>5', '<3' into (min, max)."""
    if range_str is None:
        return (float('-inf'), float('inf'))
    
    s = str(range_str).strip().lower()
    
    # Handle simple operators
    if s.startswith('>='):
        val = coerce_to_float(s[2:])
        return (val, float('inf'))
    if s.startswith('>'):
        val = coerce_to_float(s[1:])
        return (val + 1e-9, float('inf')) 
    if s.startswith('<='):
        val = coerce_to_float(s[2:])
        return (float('-inf'), val)
    if s.startswith('<'):
        val = coerce_to_float(s[1:])
        return (float('-inf'), val - 1e-9)
        
    # Handle ranges
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            min_val = coerce_to_float(parts[0])
            max_val = coerce_to_float(parts[1])
            return (min_val, max_val)
            
    # Handle exact match treated as range (rare but possible)
    val = coerce_to_float(s)
    return (val, val)

def match_fee_rule(tx_context, rule):
    """
    Checks if a transaction context matches a fee rule.
    """
    
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match - wildcard if empty/null)
    if rule.get('account_type'):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match - wildcard if empty/null)
    if rule.get('merchant_category_code'):
        if tx_context['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Boolean match - wildcard if null)
    if rule.get('is_credit') is not None:
        if bool(rule['is_credit']) != bool(tx_context['is_credit']):
            return False
            
    # 5. ACI (List match - wildcard if empty/null)
    if rule.get('aci'):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Boolean match - wildcard if null)
    if rule.get('intracountry') is not None:
        # Rule uses 1.0/0.0, context uses True/False
        rule_intra = bool(float(rule['intracountry']))
        if rule_intra != tx_context['intracountry']:
            return False
            
    # 7. Capture Delay (Complex match)
    if rule.get('capture_delay'):
        m_delay = str(tx_context['capture_delay']).lower()
        r_delay = str(rule['capture_delay']).lower()
        
        match = False
        if r_delay == m_delay:
            match = True
        elif r_delay == 'immediate' and m_delay == 'immediate':
            match = True
        elif r_delay == 'manual' and m_delay == 'manual':
            match = True
        else:
            # Handle numeric comparisons
            # Convert merchant delay to number if possible
            m_val = None
            if m_delay == 'immediate':
                m_val = 0
            elif m_delay.isdigit():
                m_val = int(m_delay)
            
            if m_val is not None:
                if r_delay.startswith('<') and m_val < coerce_to_float(r_delay[1:]):
                    match = True
                elif r_delay.startswith('>') and m_val > coerce_to_float(r_delay[1:]):
                    match = True
                elif '-' in r_delay:
                    min_d, max_d = parse_range(r_delay)
                    if min_d <= m_val <= max_d:
                        match = True
                        
        if not match:
            return False

    # 8. Monthly Volume (Range match - wildcard if null)
    if rule.get('monthly_volume'):
        min_v, max_v = parse_range(rule['monthly_volume'])
        if not (min_v <= tx_context['monthly_volume'] <= max_v):
            return False
            
    # 9. Monthly Fraud Level (Range match - wildcard if null)
    if rule.get('monthly_fraud_level'):
        min_f, max_f = parse_range(rule['monthly_fraud_level'])
        # Context is ratio (0.005), Rule is parsed from "0.5%" -> 0.005
        # Use a small epsilon for float comparison safety if needed, but direct comparison usually ok
        if not (min_f <= tx_context['monthly_fraud_level'] <= max_f):
            return False

    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    # Fee = Fixed + (Rate * Amount / 10000)
    return fixed + (rate * amount / 10000)

# ═══════════════════════════════════════════════════════════
# MAIN SCRIPT
# ═══════════════════════════════════════════════════════════

# File paths
payments_path = '/output/chunk2/data/context/payments.csv'
fees_path = '/output/chunk2/data/context/fees.json'
merchant_data_path = '/output/chunk2/data/context/merchant_data.json'

# 1. Load Data
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_data_path, 'r') as f:
    merchant_data_list = json.load(f)

# Convert merchant data to dict for easy lookup
merchant_lookup = {m['merchant']: m for m in merchant_data_list}

# 2. Filter for Target Merchant and Time Period (July 2023)
target_merchant = 'Martinis_Fine_Steakhouse'
start_day = 182
end_day = 212
year = 2023

# Filter for July transactions
df_july = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == year) &
    (df_payments['day_of_year'] >= start_day) & 
    (df_payments['day_of_year'] <= end_day)
].copy()

if df_july.empty:
    print("No transactions found for this merchant in July 2023.")
    exit()

# 3. Calculate Monthly Stats (Volume and Fraud)
monthly_volume = df_july['eur_amount'].sum()

# Fraud Level: Ratio of fraudulent volume over total volume
fraud_volume = df_july[df_july['has_fraudulent_dispute'] == True]['eur_amount'].sum()
monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 4. Get Merchant Static Attributes
m_data = merchant_lookup.get(target_merchant)
if not m_data:
    print(f"Merchant {target_merchant} not found in merchant_data.json")
    exit()

account_type = m_data.get('account_type')
mcc = m_data.get('merchant_category_code')
capture_delay = m_data.get('capture_delay')

# 5. Calculate Fees per Transaction
total_fees = 0.0
matched_count = 0
unmatched_count = 0

for _, tx in df_july.iterrows():
    # Build Transaction Context
    # Intracountry: Issuer Country == Acquirer Country
    is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
    
    tx_context = {
        'card_scheme': tx['card_scheme'],
        'account_type': account_type,
        'merchant_category_code': mcc,
        'is_credit': tx['is_credit'],
        'aci': tx['aci'],
        'intracountry': is_intracountry,
        'capture_delay': capture_delay,
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    # Find Matching Rule
    matched_rule = None
    for rule in fees_data:
        if match_fee_rule(tx_context, rule):
            matched_rule = rule
            break
            
    if matched_rule:
        fee = calculate_fee(tx['eur_amount'], matched_rule)
        total_fees += fee
        matched_count += 1
    else:
        unmatched_count += 1

# 6. Output Result
# print(f"Matched: {matched_count}, Unmatched: {unmatched_count}")
# print(f"Volume: {monthly_volume}, Fraud: {monthly_fraud_level}")
print(f"{total_fees:.2f}")
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
        v = v.lstrip('><≤≥')
        if '%' in v:
            return float(v.replace('%', '')) / 100
        return float(v)
    return float(value)

def get_month(day_of_year):
    """Convert day_of_year to month (1-12) for non-leap year 2023."""
    # Cumulative days at start of each month
    days = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]
    for i, d in enumerate(days[1:], 1):
        if day_of_year <= d:
            return i
    return 12

def check_volume_rule(rule_vol_str, actual_vol):
    """Check if actual volume falls within rule range (e.g., '100k-1m')."""
    if not rule_vol_str:
        return True
    
    s = rule_vol_str.lower().replace('€', '').strip()
    
    if '-' in s:
        parts = s.split('-')
        
        def parse_k_m(val_str):
            val_str = val_str.strip()
            mult = 1
            if 'k' in val_str:
                mult = 1000
                val_str = val_str.replace('k', '')
            elif 'm' in val_str:
                mult = 1000000
                val_str = val_str.replace('m', '')
            return float(val_str) * mult
            
        lower = parse_k_m(parts[0])
        upper = parse_k_m(parts[1])
        return lower <= actual_vol <= upper
        
    return True

def check_fraud_rule(rule_fraud_str, actual_fraud_rate):
    """Check if actual fraud rate falls within rule range (e.g., '>8.3%')."""
    if not rule_fraud_str:
        return True
    
    s = rule_fraud_str.strip()
    
    # Handle ">8.3%"
    if s.startswith('>'):
        val = float(s[1:].replace('%', '')) / 100
        return actual_fraud_rate > val
    
    # Handle "7.7%-8.3%"
    if '-' in s:
        parts = s.split('-')
        low = float(parts[0].replace('%', '')) / 100
        high = float(parts[1].replace('%', '')) / 100
        return low <= actual_fraud_rate <= high
        
    return True

def match_fee_rule(ctx, rule):
    """Determine if a fee rule applies to a specific transaction context."""
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List in rule: must contain ctx value)
    # If rule['account_type'] is empty/null, it applies to ALL account types
    if rule['account_type'] and ctx['account_type'] not in rule['account_type']:
        return False
            
    # 3. Merchant Category Code (List in rule: must contain ctx value)
    # If rule['merchant_category_code'] is empty/null, it applies to ALL MCCs
    if rule['merchant_category_code'] and ctx['mcc'] not in rule['merchant_category_code']:
        return False
            
    # 4. Is Credit (Bool match)
    # If rule['is_credit'] is None, it applies to BOTH credit and debit
    if rule['is_credit'] is not None and rule['is_credit'] != ctx['is_credit']:
        return False
            
    # 5. ACI (List in rule: must contain ctx value)
    # If rule['aci'] is empty/null, it applies to ALL ACIs
    if rule['aci'] and ctx['aci'] not in rule['aci']:
        return False
            
    # 6. Intracountry (Bool match)
    # If rule['intracountry'] is None, it applies to BOTH
    if rule['intracountry'] is not None:
        # Convert rule value to bool for comparison (0.0 -> False, 1.0 -> True)
        rule_intra = bool(rule['intracountry'])
        if rule_intra != ctx['intracountry']:
            return False
            
    # 7. Capture Delay (String/Range match)
    if rule['capture_delay']:
        rd = rule['capture_delay']
        cd = str(ctx['capture_delay'])
        
        if rd == cd:
            pass # Exact match (e.g., 'manual' == 'manual')
        elif rd == '>5':
            if not cd.isdigit() or int(cd) <= 5: return False
        elif rd == '<3':
            if not cd.isdigit() or int(cd) >= 3: return False
        elif rd == '3-5':
            if not cd.isdigit() or not (3 <= int(cd) <= 5): return False
        elif rd in ['manual', 'immediate'] and rd != cd:
            return False # Specific string mismatch
        # If none of the above, assume mismatch if strings differ
        elif rd != cd:
            return False

    # 8. Monthly Volume
    if not check_volume_rule(rule['monthly_volume'], ctx['monthly_volume']):
        return False
        
    # 9. Monthly Fraud Level
    if not check_fraud_rule(rule['monthly_fraud_level'], ctx['monthly_fraud_rate']):
        return False
        
    return True

def calculate_fee(amount, rule):
    """Calculate fee: Fixed + (Rate * Amount / 10000)"""
    return rule['fixed_amount'] + (rule['rate'] * amount / 10000)

# --- Main Execution ---

# 1. Load Data
payments_path = '/output/chunk4/data/context/payments.csv'
fees_path = '/output/chunk4/data/context/fees.json'
merchant_path = '/output/chunk4/data/context/merchant_data.json'

df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Filter Transactions
target_merchant = 'Crossfit_Hanna'
target_year = 2023
df = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

# 3. Get Merchant Metadata
m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not m_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

original_mcc = m_info['merchant_category_code']
account_type = m_info['account_type']
capture_delay = m_info['capture_delay']

# 4. Enrich Data
# Add Month
df['month'] = df['day_of_year'].apply(get_month)

# Add Intracountry (True if issuing == acquirer)
df['is_intracountry'] = df['issuing_country'] == df['acquirer_country']

# Calculate Monthly Stats (Volume and Fraud Rate by Volume)
monthly_stats = {}
for month in range(1, 13):
    month_txs = df[df['month'] == month]
    if len(month_txs) > 0:
        vol = month_txs['eur_amount'].sum()
        # Fraud Rate = Fraud Volume / Total Volume (per Manual Section 7)
        fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum()
        fraud_rate = fraud_vol / vol if vol > 0 else 0.0
        
        monthly_stats[month] = {
            'volume': vol,
            'fraud_rate': fraud_rate
        }
    else:
        monthly_stats[month] = {'volume': 0.0, 'fraud_rate': 0.0}

# 5. Calculate Fees
total_fees_old = 0.0
total_fees_new = 0.0
new_mcc = 5911

# Iterate through every transaction
for _, row in df.iterrows():
    month = row['month']
    stats = monthly_stats.get(month, {'volume': 0, 'fraud_rate': 0})
    
    # Base Context
    ctx = {
        'card_scheme': row['card_scheme'],
        'account_type': account_type,
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'intracountry': row['is_intracountry'],
        'capture_delay': capture_delay,
        'monthly_volume': stats['volume'],
        'monthly_fraud_rate': stats['fraud_rate'],
        'eur_amount': row['eur_amount']
    }
    
    # --- Scenario 1: Original MCC ---
    ctx['mcc'] = original_mcc
    fee_old = 0.0
    found_old = False
    for rule in fees:
        if match_fee_rule(ctx, rule):
            fee_old = calculate_fee(ctx['eur_amount'], rule)
            found_old = True
            break # Apply first matching rule
    
    # Fallback if no rule matches (should not happen with complete ruleset, but good for safety)
    if not found_old:
        # Assuming 0 or some default if no rule matches, but typically one should match.
        # For this exercise, we assume coverage.
        pass
        
    total_fees_old += fee_old
    
    # --- Scenario 2: New MCC (5911) ---
    ctx['mcc'] = new_mcc
    fee_new = 0.0
    found_new = False
    for rule in fees:
        if match_fee_rule(ctx, rule):
            fee_new = calculate_fee(ctx['eur_amount'], rule)
            found_new = True
            break # Apply first matching rule
            
    total_fees_new += fee_new

# 6. Calculate Delta
# Delta = New Fees - Old Fees
delta = total_fees_new - total_fees_old

# Output with high precision
print(f"{delta:.14f}")
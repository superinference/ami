# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2571
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7051 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# ═══════════════════════════════════════════════════════════
# Helper Functions
# ═══════════════════════════════════════════════════════════

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
    """Parses strings like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if not range_str or pd.isna(range_str) or range_str == 'None':
        return None, None
    
    s = str(range_str).lower().replace(',', '').replace('%', '').replace('€', '').replace('$', '')
    
    def parse_val(v):
        try:
            if 'k' in v: return float(v.replace('k', '')) * 1000
            if 'm' in v: return float(v.replace('m', '')) * 1000000
            return float(v)
        except:
            return 0.0

    try:
        if '-' in s:
            parts = s.split('-')
            return parse_val(parts[0]), parse_val(parts[1])
        elif '>' in s:
            return parse_val(s.replace('>', '')), float('inf')
        elif '<' in s:
            return float('-inf'), parse_val(s.replace('<', ''))
        else:
            val = parse_val(s)
            return val, val
    except:
        return None, None

def check_capture_delay(merchant_delay, fee_delay):
    """Checks if merchant capture delay matches fee rule."""
    if not fee_delay or fee_delay == 'None':
        return True
    
    m_delay = str(merchant_delay).lower()
    f_delay = str(fee_delay).lower()
    
    # Direct match
    if m_delay == f_delay:
        return True
        
    # Logic mapping based on manual/common sense
    # Merchant values: '1', '2', '7', 'immediate', 'manual'
    # Fee values: '3-5', '<3', '>5', 'immediate', 'manual'
    
    if f_delay == '<3':
        return m_delay in ['1', '2', 'immediate']
    if f_delay == '>5':
        return m_delay in ['7', 'manual'] # Assuming manual is long delay
    if f_delay == '3-5':
        return m_delay in ['3', '4', '5']
        
    return False

# ═══════════════════════════════════════════════════════════
# Main Analysis
# ═══════════════════════════════════════════════════════════

# 1. Load Data
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'
payments_path = '/output/chunk2/data/context/payments.csv'

with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
df_payments = pd.read_csv(payments_path)

# 2. Get Fee Rule 787
target_fee_id = 787
fee_rule = next((f for f in fees_data if f['ID'] == target_fee_id), None)

if not fee_rule:
    print("Fee 787 not found.")
    exit()

print(f"Analyzing Fee {target_fee_id}...")
# print(json.dumps(fee_rule, indent=2))

# 3. Identify Merchants with Account Type 'R'
# The question imposes the condition: "only applied to account type R"
r_merchants = [m for m in merchant_data if m.get('account_type') == 'R']
r_merchant_names = [m['merchant'] for m in r_merchants]

if not r_merchant_names:
    print("No merchants with account type 'R' found.")
    exit()

# 4. Filter Merchants based on Fee 787 Criteria
affected_merchants = []

for merchant in r_merchants:
    name = merchant['merchant']
    
    # --- Check Merchant-Level Static Criteria ---
    
    # Merchant Category Code (MCC)
    if fee_rule['merchant_category_code']:
        if merchant['merchant_category_code'] not in fee_rule['merchant_category_code']:
            continue # MCC doesn't match
            
    # Capture Delay
    if not check_capture_delay(merchant['capture_delay'], fee_rule['capture_delay']):
        continue # Capture delay doesn't match

    # --- Check Merchant-Level Aggregate Criteria (Volume/Fraud) ---
    # We need to calculate these from payments data for the specific merchant
    merchant_txs = df_payments[df_payments['merchant'] == name]
    
    if merchant_txs.empty:
        continue

    # Monthly Volume (Average for 2023)
    if fee_rule['monthly_volume']:
        total_vol = merchant_txs['eur_amount'].sum()
        avg_monthly_vol = total_vol / 12.0
        min_v, max_v = parse_range(fee_rule['monthly_volume'])
        if min_v is not None:
            if not (min_v <= avg_monthly_vol <= max_v):
                continue # Volume doesn't match

    # Monthly Fraud Level (Average for 2023)
    if fee_rule['monthly_fraud_level']:
        fraud_count = merchant_txs['has_fraudulent_dispute'].sum()
        total_count = len(merchant_txs)
        fraud_rate = fraud_count / total_count if total_count > 0 else 0.0
        min_f, max_f = parse_range(fee_rule['monthly_fraud_level'])
        # parse_range handles % (e.g. 8.3% -> 0.083)
        if min_f is not None:
            if not (min_f <= fraud_rate <= max_f):
                continue # Fraud level doesn't match

    # --- Check Transaction-Level Criteria ---
    # The merchant is "affected" if they have ANY transactions that would trigger this fee
    # Filter the merchant's transactions by fee criteria
    
    matching_txs = merchant_txs.copy()
    
    # Card Scheme
    if fee_rule['card_scheme']:
        matching_txs = matching_txs[matching_txs['card_scheme'] == fee_rule['card_scheme']]
        
    # Is Credit
    if fee_rule['is_credit'] is not None:
        # Handle boolean/string differences
        target_credit = bool(fee_rule['is_credit'])
        matching_txs = matching_txs[matching_txs['is_credit'] == target_credit]
        
    # ACI
    if fee_rule['aci']:
        # Fee ACI is a list, Transaction ACI is a string
        matching_txs = matching_txs[matching_txs['aci'].isin(fee_rule['aci'])]
        
    # Intracountry
    if fee_rule['intracountry'] is not None:
        # 1.0/True = Domestic (Issuing == Acquirer)
        # 0.0/False = International (Issuing != Acquirer)
        target_intra = bool(float(fee_rule['intracountry'])) # Handle 0.0/1.0
        
        is_domestic = matching_txs['issuing_country'] == matching_txs['acquirer_country']
        if target_intra:
            matching_txs = matching_txs[is_domestic]
        else:
            matching_txs = matching_txs[~is_domestic]

    # If any transactions remain, the merchant is affected
    if not matching_txs.empty:
        affected_merchants.append(name)

# 5. Output Results
# Return unique list of merchant names
unique_affected = sorted(list(set(affected_merchants)))
print(", ".join(unique_affected))

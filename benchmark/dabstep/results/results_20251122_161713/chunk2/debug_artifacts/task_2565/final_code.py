import pandas as pd
import json

# Helper function to safely coerce values (standard robust helper)
def coerce_to_float(value):
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
        return float(v)
    return float(value)

# Helper to match capture delay logic
def match_capture_delay(merchant_val, rule_val):
    """Matches merchant capture delay against rule."""
    if rule_val is None:
        return True
    
    # Direct string match (e.g., 'manual' == 'manual')
    if str(merchant_val) == str(rule_val):
        return True
        
    # Numeric logic for ranges like '>5', '<3'
    m_days = None
    if str(merchant_val).isdigit():
        m_days = float(merchant_val)
        
    if rule_val == '>5':
        return m_days is not None and m_days > 5
    if rule_val == '<3':
        return (m_days is not None and m_days < 3) or merchant_val == 'immediate'
    if rule_val == '3-5':
        return m_days is not None and 3 <= m_days <= 5
    if rule_val == 'immediate':
        return merchant_val == 'immediate'
    if rule_val == 'manual':
        return merchant_val == 'manual'
        
    return False

# Load Data Files
fees_path = '/output/chunk2/data/context/fees.json'
merchant_path = '/output/chunk2/data/context/merchant_data.json'
payments_path = '/output/chunk2/data/context/payments.csv'

with open(fees_path, 'r') as f:
    fees = json.load(f)
with open(merchant_path, 'r') as f:
    merchants = json.load(f)
payments = pd.read_csv(payments_path)

# 1. Get Fee 17 Details
fee_17 = next((f for f in fees if f['ID'] == 17), None)
if not fee_17:
    print("Fee 17 not found")
    exit()

# 2. Prepare Merchant Lookup Map
merchant_map = {m['merchant']: m for m in merchants}

# 3. Filter Payments by Fee 17's Transaction-Level Criteria
# This identifies merchants who processed transactions eligible for this fee
filtered_payments = payments.copy()

# Filter by Card Scheme
if fee_17['card_scheme']:
    filtered_payments = filtered_payments[filtered_payments['card_scheme'] == fee_17['card_scheme']]

# Filter by Credit/Debit
if fee_17['is_credit'] is not None:
    filtered_payments = filtered_payments[filtered_payments['is_credit'] == fee_17['is_credit']]

# Filter by ACI (Authorization Characteristics Indicator)
if fee_17['aci']:
    # Fee rule 'aci' is a list of allowed values
    filtered_payments = filtered_payments[filtered_payments['aci'].isin(fee_17['aci'])]

# Filter by Intracountry (Issuer Country == Acquirer Country)
if fee_17['intracountry'] is not None:
    is_intra = filtered_payments['issuing_country'] == filtered_payments['acquirer_country']
    if fee_17['intracountry']:
        filtered_payments = filtered_payments[is_intra]
    else:
        filtered_payments = filtered_payments[~is_intra]

# 4. Identify Affected Merchants
# Logic: Merchant matches OLD Fee 17 -> Merchant does NOT match NEW Fee 17 (Account Type != 'R')
affected_merchants = set()
candidate_merchants = filtered_payments['merchant'].unique()

for m_name in candidate_merchants:
    m_info = merchant_map.get(m_name)
    if not m_info:
        continue
        
    # Check Merchant-Level Criteria for OLD Fee 17
    
    # MCC Check
    if fee_17['merchant_category_code']:
        if m_info['merchant_category_code'] not in fee_17['merchant_category_code']:
            continue
            
    # Capture Delay Check
    if not match_capture_delay(m_info['capture_delay'], fee_17['capture_delay']):
        continue
        
    # Old Account Type Check (if Fee 17 already had restrictions)
    if fee_17['account_type']:
        if m_info['account_type'] not in fee_17['account_type']:
            continue
            
    # If we reach here, the merchant matches the ORIGINAL Fee 17.
    # Now check if they are AFFECTED by the change (Restriction to 'R').
    # They are affected if their account type is NOT 'R'.
    
    if m_info['account_type'] != 'R':
        affected_merchants.add(m_name)

# 5. Output Result
# Return list of merchant names
result = sorted(list(affected_merchants))
print(", ".join(result))
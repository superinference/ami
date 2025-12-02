import pandas as pd
import json
import numpy as np
import re

# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
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
    return 0.0

def parse_range_check(value, range_str):
    """
    Check if a numeric value falls within a range string (e.g., '100k-1m', '>5', '<3%').
    Handles k/m suffixes and percentages.
    """
    if range_str is None:
        return True
        
    # Normalize range string
    s = str(range_str).lower().strip()
    
    # Helper to parse single number with units
    def parse_num(n_str):
        n_str = n_str.replace('%', '')
        mult = 1
        if 'k' in n_str:
            mult = 1000
            n_str = n_str.replace('k', '')
        elif 'm' in n_str:
            mult = 1000000
            n_str = n_str.replace('m', '')
        try:
            val = float(n_str)
            if '%' in str(range_str): # If original had %, scale down
                val = val / 100
            else:
                val = val * mult
            return val
        except ValueError:
            return 0.0

    val = float(value)
    
    if '-' in s:
        parts = s.split('-')
        low = parse_num(parts[0])
        high = parse_num(parts[1])
        return low <= val <= high
    elif s.startswith('>'):
        limit = parse_num(s[1:])
        return val > limit
    elif s.startswith('<'):
        limit = parse_num(s[1:])
        return val < limit
    else:
        # Exact match attempt (rare for ranges)
        return val == parse_num(s)

def map_capture_delay(merchant_delay):
    """Map merchant capture delay values to fee rule format."""
    # Merchant values: '1', '2', '7', 'immediate', 'manual'
    # Fee values: '<3', '3-5', '>5', 'immediate', 'manual'
    md = str(merchant_delay).lower()
    if md == 'manual': return 'manual'
    if md == 'immediate': return 'immediate'
    
    try:
        days = float(md)
        if days < 3: return '<3'
        if 3 <= days <= 5: return '3-5'
        if days > 5: return '>5'
    except ValueError:
        pass
    return md # Fallback

def match_fee_rule(tx_ctx, rule, candidate_aci):
    """
    Check if a fee rule applies to a transaction context with a specific candidate ACI.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') != tx_ctx['card_scheme']:
        return False

    # 2. Account Type (List match or Wildcard)
    if rule.get('account_type'):
        if tx_ctx['merchant_account_type'] not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match or Wildcard)
    if rule.get('merchant_category_code'):
        if tx_ctx['merchant_mcc'] not in rule['merchant_category_code']:
            return False

    # 4. Capture Delay (Mapped match or Wildcard)
    if rule.get('capture_delay'):
        mapped_delay = map_capture_delay(tx_ctx['merchant_capture_delay'])
        if rule['capture_delay'] != mapped_delay:
            return False

    # 5. Monthly Volume (Range match or Wildcard)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 6. Monthly Fraud Level (Range match or Wildcard)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False

    # 7. Is Credit (Boolean match or Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 8. ACI (List match or Wildcard) - CHECKING CANDIDATE ACI
    if rule.get('aci'):
        if candidate_aci not in rule['aci']:
            return False

    # 9. Intracountry (Boolean match or Wildcard)
    if rule.get('intracountry') is not None:
        # Fee rule uses 0.0/1.0, tx uses boolean
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False

    return True

# ---------------------------------------------------------
# MAIN LOGIC
# ---------------------------------------------------------

# 1. Load Data
payments_path = '/output/chunk3/data/context/payments.csv'
merchant_path = '/output/chunk3/data/context/merchant_data.json'
fees_path = '/output/chunk3/data/context/fees.json'

df_payments = pd.read_csv(payments_path)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)

# 2. Setup Target
target_merchant = 'Crossfit_Hanna'
target_year = 2023

# 3. Get Merchant Static Info
m_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not m_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

# 4. Prepare Monthly Stats (Volume & Fraud Rate) for Fee Rules
# Filter for ALL transactions of this merchant in 2023 to calculate correct stats
df_merchant_2023 = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

# Convert day_of_year to month (2023 is non-leap)
def get_month(doy):
    return (pd.to_datetime(doy - 1, unit='D', origin=f'{target_year}-01-01')).month

df_merchant_2023['month'] = df_merchant_2023['day_of_year'].apply(get_month)

monthly_stats = {}
for month in range(1, 13):
    m_df = df_merchant_2023[df_merchant_2023['month'] == month]
    if len(m_df) == 0:
        monthly_stats[month] = {'vol': 0.0, 'fraud_rate': 0.0}
    else:
        total_vol = m_df['eur_amount'].sum()
        fraud_vol = m_df[m_df['has_fraudulent_dispute'] == True]['eur_amount'].sum()
        # Manual: "ratio between monthly total volume and monthly volume notified as fraud"
        # Usually fraud rate is fraud/total.
        fraud_rate = (fraud_vol / total_vol) if total_vol > 0 else 0.0
        monthly_stats[month] = {'vol': total_vol, 'fraud_rate': fraud_rate}

# 5. Filter Target Transactions (Fraudulent ones to re-price)
df_fraud = df_merchant_2023[df_merchant_2023['has_fraudulent_dispute'] == True].copy()

# 6. Simulate Costs for each ACI
candidate_acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
aci_total_costs = {aci: 0.0 for aci in candidate_acis}
aci_validity = {aci: True for aci in candidate_acis} # Track if ACI is possible for all txs

print(f"Analyzing {len(df_fraud)} fraudulent transactions for optimization...")

for idx, tx in df_fraud.iterrows():
    month = tx['month']
    stats = monthly_stats.get(month, {'vol': 0, 'fraud_rate': 0})
    
    # Context for this transaction
    tx_ctx = {
        'card_scheme': tx['card_scheme'],
        'is_credit': bool(tx['is_credit']),
        'intracountry': tx['issuing_country'] == tx['acquirer_country'],
        'eur_amount': float(tx['eur_amount']),
        'merchant_account_type': m_info['account_type'],
        'merchant_mcc': m_info['merchant_category_code'],
        'merchant_capture_delay': m_info['capture_delay'],
        'monthly_volume': stats['vol'],
        'monthly_fraud_rate': stats['fraud_rate']
    }
    
    # Test each ACI
    for aci in candidate_acis:
        if not aci_validity[aci]:
            continue # Skip if already proven invalid
            
        # Find first matching rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(tx_ctx, rule, aci):
                matched_rule = rule
                break
        
        if matched_rule:
            # Calculate Fee: Fixed + (Rate * Amount / 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx_ctx['eur_amount'] / 10000)
            aci_total_costs[aci] += fee
        else:
            # If no rule matches, this ACI is not available for this transaction type
            # Mark as invalid for the preferred choice
            aci_validity[aci] = False
            aci_total_costs[aci] = float('inf')

# 7. Determine Preferred Choice
# Filter out invalid ACIs
valid_results = {k: v for k, v in aci_total_costs.items() if aci_validity[k]}

if not valid_results:
    print("No valid ACI found that covers all transactions.")
else:
    best_aci = min(valid_results, key=valid_results.get)
    min_cost = valid_results[best_aci]
    
    # Debug output
    # print("Costs per ACI:", {k: round(v, 2) for k, v in valid_results.items()})
    
    # Final Answer
    print(best_aci)
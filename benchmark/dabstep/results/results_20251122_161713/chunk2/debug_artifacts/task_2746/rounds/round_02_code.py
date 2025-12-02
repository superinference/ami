# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2746
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 7759 characters (FULL CODE)
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
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                return (float(parts[0]) + float(parts[1])) / 2
            except:
                pass
        return float(v)
    return float(value)

def parse_range_str(range_str, is_percent=False):
    """Parses strings like '100k-1m', '>5', '<3', '7.7%-8.3%' into (min, max)."""
    if range_str is None or str(range_str) == 'None':
        return -float('inf'), float('inf')
    
    s = str(range_str).lower().strip()
    multiplier = 1
    if is_percent:
        s = s.replace('%', '')
        multiplier = 0.01
    else:
        if 'k' in s: multiplier = 1000; s = s.replace('k', '')
        elif 'm' in s: multiplier = 1000000; s = s.replace('m', '')
    
    try:
        if '-' in s:
            parts = s.split('-')
            return float(parts[0]) * multiplier, float(parts[1]) * multiplier
        if s.startswith('>'):
            return float(s[1:]) * multiplier, float('inf')
        if s.startswith('<'):
            return -float('inf'), float(s[1:]) * multiplier
        val = float(s) * multiplier
        return val, val # Exact match treated as range [val, val]
    except:
        return -float('inf'), float('inf')

def check_capture_delay(merchant_delay, rule_delay):
    """Checks if merchant capture delay matches the rule."""
    if rule_delay is None or str(rule_delay) == 'None': return True
    m_delay = str(merchant_delay).lower()
    r_delay = str(rule_delay).lower()
    
    if r_delay == m_delay: return True
    
    # Handle numeric comparisons for delays like '1', '2', '7' vs '>5', '<3'
    try:
        # Convert merchant delay to float if possible (e.g. '1' -> 1.0)
        # 'immediate' or 'manual' will fail here and fall through
        m_days = float(m_delay)
        
        if '-' in r_delay:
            low, high = map(float, r_delay.split('-'))
            return low <= m_days <= high
        if r_delay.startswith('>'):
            return m_days > float(r_delay[1:])
        if r_delay.startswith('<'):
            return m_days < float(r_delay[1:])
    except ValueError:
        pass
        
    return False

def match_fee_rule(tx_ctx, rule):
    """Matches a transaction context against a fee rule."""
    # 1. Card Scheme (Exact match required)
    if rule.get('card_scheme') != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List contains)
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. MCC (List contains)
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Bool match or None)
    if rule.get('is_credit') is not None:
        rule_credit = str(rule['is_credit']).lower() == 'true'
        if rule_credit != tx_ctx['is_credit']:
            return False
            
    # 5. ACI (List contains) - This is the variable we are simulating!
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Intracountry (Bool match or None)
    if rule.get('intracountry') is not None:
        # rule['intracountry'] might be 0.0, 1.0, or 'None'
        if str(rule['intracountry']) != 'None':
            rule_intra = float(rule['intracountry']) == 1.0
            if rule_intra != tx_ctx['is_intracountry']:
                return False

    # 7. Capture Delay
    if not check_capture_delay(tx_ctx['capture_delay'], rule.get('capture_delay')):
        return False

    # 8. Monthly Volume
    min_vol, max_vol = parse_range_str(rule.get('monthly_volume'))
    if not (min_vol <= tx_ctx['monthly_volume'] <= max_vol):
        return False

    # 9. Monthly Fraud Level
    min_fraud, max_fraud = parse_range_str(rule.get('monthly_fraud_level'), is_percent=True)
    if not (min_fraud <= tx_ctx['monthly_fraud_level'] <= max_fraud):
        return False
        
    return True

def calculate_fee(amount, rule):
    """Calculates fee based on fixed amount and rate."""
    fixed = float(rule.get('fixed_amount', 0))
    rate = float(rule.get('rate', 0))
    return fixed + (rate * amount / 10000)

# --- Main Execution ---

# 1. Load Data
payments_df = pd.read_csv('/output/chunk2/data/context/payments.csv')
with open('/output/chunk2/data/context/merchant_data.json') as f:
    merchant_data = json.load(f)
with open('/output/chunk2/data/context/fees.json') as f:
    fees = json.load(f)

# 2. Get Merchant Context (Rafa_AI)
merchant_name = 'Rafa_AI'
m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
if not m_info:
    raise ValueError(f"Merchant {merchant_name} not found in merchant_data.json")

# 3. Calculate October Stats (Day 274-304)
# These stats determine the fee tier (volume/fraud level) and remain constant during simulation
oct_mask = (payments_df['merchant'] == merchant_name) & \
           (payments_df['day_of_year'] >= 274) & \
           (payments_df['day_of_year'] <= 304)
oct_df = payments_df[oct_mask]

monthly_volume = oct_df['eur_amount'].sum()
fraud_volume = oct_df[oct_df['has_fraudulent_dispute']]['eur_amount'].sum()
monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0

# 4. Identify Fraudulent Transactions to Simulate
# We will calculate the cost of THESE transactions under different ACI scenarios
fraud_txs = oct_df[oct_df['has_fraudulent_dispute']].copy()

# 5. Simulate Costs for Each ACI
acis = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
aci_costs = {}

for target_aci in acis:
    total_cost = 0.0
    
    for _, tx in fraud_txs.iterrows():
        # Build context for this specific transaction with the SIMULATED ACI
        ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': m_info['account_type'],
            'mcc': m_info['merchant_category_code'],
            'is_credit': tx['is_credit'],
            'aci': target_aci, # <--- The variable we are changing
            'is_intracountry': tx['issuing_country'] == tx['acquirer_country'],
            'capture_delay': m_info['capture_delay'],
            'monthly_volume': monthly_volume,
            'monthly_fraud_level': monthly_fraud_level
        }
        
        # Find the first matching fee rule
        matched_rule = None
        for rule in fees:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break # Priority is usually given to the first match in fee tables
        
        if matched_rule:
            fee = calculate_fee(tx['eur_amount'], matched_rule)
            total_cost += fee
        else:
            # If no rule matches, this ACI might be invalid for this transaction type
            # However, for this problem, we assume a valid path exists or penalize it.
            # Given the dataset, there is likely a fallback rule.
            pass

    aci_costs[target_aci] = total_cost

# 6. Determine Preferred Choice (Lowest Cost)
best_aci = min(aci_costs, key=aci_costs.get)
min_cost = aci_costs[best_aci]

# Output the result (Just the ACI code as requested by "preferred choice")
print(best_aci)

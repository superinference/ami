# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1301
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 8719 characters (FULL CODE)
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
        try:
            return float(v)
        except:
            return 0.0
    return 0.0

def parse_range_value(val_str):
    """Parses strings like '100k', '1m', '8.3%' into floats for comparison."""
    if not isinstance(val_str, str):
        return val_str
    val_str = val_str.lower().strip()
    if val_str.endswith('%'):
        return float(val_str.rstrip('%')) / 100.0
    if val_str.endswith('k'):
        return float(val_str.rstrip('k')) * 1000.0
    if val_str.endswith('m'):
        return float(val_str.rstrip('m')) * 1000000.0
    try:
        return float(val_str)
    except:
        return 0.0

def check_range(rule_range_str, actual_value):
    """
    Checks if actual_value fits in rule_range_str.
    Handles:
    - Ranges: '100k-1m', '7.7%-8.3%'
    - Inequalities: '>5', '<3'
    - Categorical exact matches: 'immediate', 'manual'
    - Numeric exact matches: '1'
    """
    if rule_range_str is None:
        return True
    
    # Handle categorical exact matches (e.g., capture_delay)
    if rule_range_str in ['immediate', 'manual']:
        return str(actual_value) == rule_range_str
    
    # If actual value is categorical but rule is numeric/range
    if str(actual_value) in ['immediate', 'manual']:
        # If rule is numeric (e.g. '<3'), 'immediate' or 'manual' cannot be compared numerically
        # unless we map them. Based on context, usually they are distinct categories.
        # However, 'immediate' implies 0 delay. 'manual' implies infinite.
        # Let's try to map for robustness if needed, but usually exact match handles it.
        # If rule is '<3' and value is 'immediate', technically immediate is < 3 days.
        # Let's map 'immediate' to 0 for numeric comparison.
        if str(actual_value) == 'immediate':
            val = 0.0
        else:
            return False # 'manual' vs numeric rule -> False
    else:
        # Numeric comparisons
        try:
            val = float(actual_value)
        except:
            return False # Cannot compare non-numeric actual against numeric rule

    s = str(rule_range_str).strip()
    
    if '-' in s:
        parts = s.split('-')
        low = parse_range_value(parts[0])
        high = parse_range_value(parts[1])
        return low <= val <= high
    
    if s.startswith('>'):
        limit = parse_range_value(s[1:])
        return val > limit
    
    if s.startswith('<'):
        limit = parse_range_value(s[1:])
        return val < limit
        
    # Exact numeric match?
    return val == parse_range_value(s)

def match_fee_rule(ctx, rule):
    """
    Matches a transaction context against a fee rule.
    Returns True if the rule applies, False otherwise.
    """
    # 1. Card Scheme (Exact match required)
    if rule.get('card_scheme') and rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Is Credit (Boolean match required if not null)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False
            
    # 3. Intracountry (Boolean match required if not null)
    if rule.get('intracountry') is not None:
        if rule['intracountry'] != ctx['intracountry']:
            return False
            
    # 4. Merchant Category Code (List membership)
    if rule.get('merchant_category_code'):
        if ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 5. Account Type (List membership)
    if rule.get('account_type'):
        if ctx['account_type'] not in rule['account_type']:
            return False
            
    # 6. ACI (List membership)
    if rule.get('aci'):
        if ctx['aci'] not in rule['aci']:
            return False
            
    # 7. Monthly Volume (Range check)
    if rule.get('monthly_volume'):
        if not check_range(rule['monthly_volume'], ctx['monthly_volume']):
            return False
            
    # 8. Monthly Fraud Level (Range check)
    if rule.get('monthly_fraud_level'):
        if not check_range(rule['monthly_fraud_level'], ctx['monthly_fraud_level']):
            return False
            
    # 9. Capture Delay (Range/Categorical check)
    if rule.get('capture_delay'):
        if not check_range(rule['capture_delay'], ctx['capture_delay']):
            return False
            
    return True

def execute_step():
    # 1. Load Data
    payments = pd.read_csv('/output/chunk3/data/context/payments.csv')
    with open('/output/chunk3/data/context/fees.json') as f:
        fees = json.load(f)
    with open('/output/chunk3/data/context/merchant_data.json') as f:
        merchant_data = json.load(f)
    
    # 2. Preprocess Payments (Dates & Stats)
    # Convert day_of_year to month (2023 is non-leap)
    payments['date'] = pd.to_datetime(payments['year'] * 1000 + payments['day_of_year'], format='%Y%j')
    payments['month'] = payments['date'].dt.month
    
    # Calculate Monthly Stats per Merchant
    # Volume: Sum of eur_amount
    # Fraud: Sum of eur_amount where has_fraudulent_dispute is True
    stats = payments.groupby(['merchant', 'month']).agg(
        monthly_volume=('eur_amount', 'sum'),
        fraud_volume=('eur_amount', lambda x: x[payments.loc[x.index, 'has_fraudulent_dispute']].sum())
    ).reset_index()
    
    stats['monthly_fraud_rate'] = stats['fraud_volume'] / stats['monthly_volume']
    stats['monthly_fraud_rate'] = stats['monthly_fraud_rate'].fillna(0.0)
    
    # Create lookup dictionary for stats: (merchant, month) -> {vol, fraud_rate}
    stats_lookup = stats.set_index(['merchant', 'month']).to_dict('index')
    
    # Create lookup dictionary for merchant static data
    m_dict = {m['merchant']: m for m in merchant_data}
    
    # 3. Filter Target Transactions
    # Question asks for "credit transactions" and "GlobalCard"
    target_txs = payments[
        (payments['card_scheme'] == 'GlobalCard') & 
        (payments['is_credit'] == True)
    ].copy()
    
    # 4. Apply Fee Rules
    # Filter fees for GlobalCard to speed up matching
    global_fees = [f for f in fees if f['card_scheme'] == 'GlobalCard']
    
    calculated_fees = []
    
    # Iterate through each historical transaction to find the applicable fee for that context
    for _, tx in target_txs.iterrows():
        merchant_name = tx['merchant']
        month = tx['month']
        
        # Retrieve Context Data
        m_info = m_dict.get(merchant_name)
        if not m_info:
            continue
            
        stat = stats_lookup.get((merchant_name, month))
        if not stat:
            continue
            
        # Construct Context Dictionary
        ctx = {
            'card_scheme': 'GlobalCard',
            'is_credit': True,
            'merchant_category_code': m_info['merchant_category_code'],
            'account_type': m_info['account_type'],
            'aci': tx['aci'],
            'intracountry': tx['issuing_country'] == tx['acquirer_country'],
            'monthly_volume': stat['monthly_volume'],
            'monthly_fraud_level': stat['monthly_fraud_rate'],
            'capture_delay': m_info['capture_delay']
        }
        
        # Find Matching Rule
        matched_rule = None
        for rule in global_fees:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break # Assume first match applies
        
        if matched_rule:
            # Calculate fee for the hypothetical transaction value of 4321 EUR
            # Formula: fee = fixed_amount + (rate * amount / 10000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * 4321.0 / 10000.0)
            calculated_fees.append(fee)
            
    # 5. Calculate and Print Average
    if calculated_fees:
        avg_fee = sum(calculated_fees) / len(calculated_fees)
        print(f"{avg_fee:.14f}")
    else:
        print("No applicable fees found")

if __name__ == "__main__":
    execute_step()

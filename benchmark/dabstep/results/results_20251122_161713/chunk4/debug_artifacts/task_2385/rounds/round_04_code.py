# ═══════════════════════════════════════════════════════════
# Round 4 - Task 2385
# ═══════════════════════════════════════════════════════════
# Temperature: 0.30
# Verification: sufficient
# Plan steps: 2
# Code length: 7326 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import math

# --- HELPER FUNCTIONS ---
def coerce_to_float(value):
    """Convert string with %, $, k, m, commas to float."""
    if value is None: return None
    if isinstance(value, (int, float)): return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '').replace('_', '')
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100.0
            except:
                return None
        if 'k' in v.lower():
            try:
                return float(v.lower().replace('k', '')) * 1000.0
            except:
                return None
        if 'm' in v.lower():
            try:
                return float(v.lower().replace('m', '')) * 1000000.0
            except:
                return None
        try:
            return float(v)
        except:
            return None
    return None

def parse_range(rule_val, actual_val):
    """
    Parses rule values like '100k-1m', '>5', '<3', '7.7%-8.3%'
    Returns True if actual_val fits in range.
    """
    if rule_val is None: return True
    if actual_val is None: return False
    
    s_rule = str(rule_val).strip()
    f_actual = coerce_to_float(actual_val)
    
    # Handle Inequalities (e.g., ">5", "<=10%")
    if s_rule.startswith(('>', '<', '>=')):
        if f_actual is None: return False
        
        # Extract number from rule
        num_str = s_rule.replace('>=', '').replace('<=', '').replace('>', '').replace('<', '')
        f_rule = coerce_to_float(num_str)
        if f_rule is None: return False
        
        if s_rule.startswith('>='): return f_actual >= f_rule
        if s_rule.startswith('>'): return f_actual > f_rule
        if s_rule.startswith('<='): return f_actual <= f_rule
        if s_rule.startswith('<'): return f_actual < f_rule
        
    # Handle Ranges (e.g. "100k-1m", "7.7%-8.3%")
    if '-' in s_rule:
        parts = s_rule.split('-')
        if len(parts) == 2:
            low = coerce_to_float(parts[0])
            high = coerce_to_float(parts[1])
            if low is not None and high is not None and f_actual is not None:
                return low <= f_actual <= high
                
    # Exact match (Numeric or String)
    f_rule_exact = coerce_to_float(s_rule)
    if f_rule_exact is not None and f_actual is not None:
        return abs(f_rule_exact - f_actual) < 1e-9
        
    return str(rule_val).lower() == str(actual_val).lower()

def match_fee_rule(ctx, rule):
    """
    Checks if a transaction context matches a fee rule.
    """
    # 1. Card Scheme
    if rule.get('card_scheme') and rule['card_scheme'] != ctx['card_scheme']:
        return False
        
    # 2. Account Type (List in rule, string in ctx)
    if rule.get('account_type'):
        if ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. MCC (List in rule, int in ctx)
    if rule.get('merchant_category_code'):
        if ctx['merchant_category_code'] not in rule['merchant_category_code']:
            return False
            
    # 4. Is Credit (Bool)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx['is_credit']:
            return False
            
    # 5. ACI (List in rule, string in ctx)
    if rule.get('aci'):
        if ctx['aci'] not in rule['aci']:
            return False
            
    # 6. Capture Delay (Range/String)
    if rule.get('capture_delay'):
        if not parse_range(rule['capture_delay'], ctx['capture_delay']):
            return False
                    
    # 7. Monthly Fraud Level (Range)
    if rule.get('monthly_fraud_level'):
        if not parse_range(rule['monthly_fraud_level'], ctx['monthly_fraud_level']):
            return False
            
    # 8. Monthly Volume (Range)
    if rule.get('monthly_volume'):
        if not parse_range(rule['monthly_volume'], ctx['monthly_volume']):
            return False
            
    # 9. Intracountry (Bool/Float)
    if rule.get('intracountry') is not None:
        r_val = rule['intracountry']
        # Handle 0.0/1.0 as bools
        r_bool = bool(r_val) if not isinstance(r_val, (int, float)) else (r_val != 0)
        if r_bool != ctx['intracountry']:
            return False
            
    return True

def main():
    # 1. Load Data
    try:
        payments = pd.read_csv('/output/chunk4/data/context/payments.csv')
        with open('/output/chunk4/data/context/fees.json') as f:
            fees = json.load(f)
        with open('/output/chunk4/data/context/merchant_data.json') as f:
            merchant_data = json.load(f)
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # 2. Filter Data: Rafa_AI, April 2023
    # April 2023 (Non-leap year): Days 91 to 120
    df = payments[
        (payments['merchant'] == 'Rafa_AI') &
        (payments['year'] == 2023) &
        (payments['day_of_year'] >= 91) &
        (payments['day_of_year'] <= 120)
    ].copy()
    
    if df.empty:
        print("No transactions found for Rafa_AI in April 2023")
        return

    # 3. Get Merchant Info
    m_info = next((m for m in merchant_data if m['merchant'] == 'Rafa_AI'), None)
    if not m_info:
        print("Merchant Rafa_AI not found in merchant_data.json")
        return
    
    # 4. Calculate Monthly Stats (Volume & Fraud) for Rule Matching
    # Note: Stats are calculated on the filtered monthly data
    monthly_vol = df['eur_amount'].sum()
    
    # Fraud: Ratio of fraudulent volume over total volume
    fraud_vol = df[df['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = (fraud_vol / monthly_vol) if monthly_vol > 0 else 0.0
    
    # 5. Get Fee Rule 17
    rule_17 = next((f for f in fees if f['ID'] == 17), None)
    if not rule_17:
        print("Rule 17 not found")
        return
        
    # 6. Find Matching Transactions and Calculate Delta
    matching_amount = 0.0
    
    for _, tx in df.iterrows():
        # Determine Intracountry (Issuer == Acquirer)
        is_intra = (tx['issuing_country'] == tx['acquirer_country'])
        
        # Build Context for Matching
        ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': m_info['account_type'],
            'merchant_category_code': m_info['merchant_category_code'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'capture_delay': m_info['capture_delay'],
            'monthly_fraud_level': fraud_rate,
            'monthly_volume': monthly_vol,
            'intracountry': is_intra
        }
        
        # Check if Rule 17 applies
        if match_fee_rule(ctx, rule_17):
            matching_amount += tx['eur_amount']
            
    # 7. Calculate Delta
    # Formula: fee = fixed + rate * amount / 10000
    # Delta = New Fee - Old Fee
    # Delta = (fixed + new_rate * amt / 10000) - (fixed + old_rate * amt / 10000)
    # Delta = (new_rate - old_rate) * amt / 10000
    
    old_rate = rule_17['rate']
    new_rate = 1
    
    # Calculate delta with high precision
    delta = (new_rate - old_rate) * matching_amount / 10000.0
    
    # Print result with high precision as requested for delta questions
    print(f"{delta:.14f}")

if __name__ == "__main__":
    main()

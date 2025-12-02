# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2385
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: insufficient
# Plan steps: 1
# Code length: 7238 characters (FULL CODE)
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
    Handles mixed types (str vs float) safely.
    """
    if rule_val is None: return True
    if actual_val is None: return False
    
    s_rule = str(rule_val).strip()
    
    # Try to convert actual_val to float for numeric comparisons
    f_actual = coerce_to_float(actual_val)
    
    # Handle Inequalities (e.g., ">5", "<=10%")
    if s_rule.startswith(('>', '<', '>=')):
        if f_actual is None: return False # Cannot compare string > number
        
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
                
    # Exact match (String or Numeric)
    # Try numeric equality first if both are numbers
    f_rule_exact = coerce_to_float(s_rule)
    if f_rule_exact is not None and f_actual is not None:
        return abs(f_rule_exact - f_actual) < 1e-9
        
    # Fallback to string comparison
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
    # Load Data
    try:
        payments = pd.read_csv('/output/chunk4/data/context/payments.csv')
        with open('/output/chunk4/data/context/fees.json') as f:
            fees = json.load(f)
        with open('/output/chunk4/data/context/merchant_data.json') as f:
            merchant_data = json.load(f)
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # Filter Rafa_AI, April 2023
    # April 2023: Day 91 to 120 (Non-leap year)
    # Jan=31, Feb=28, Mar=31 -> 90 days. April starts 91. Ends 120.
    df = payments[
        (payments['merchant'] == 'Rafa_AI') &
        (payments['year'] == 2023) &
        (payments['day_of_year'] >= 91) &
        (payments['day_of_year'] <= 120)
    ].copy()
    
    if df.empty:
        print("No transactions found for Rafa_AI in April 2023")
        return

    # Get Merchant Info
    m_info = next((m for m in merchant_data if m['merchant'] == 'Rafa_AI'), None)
    if not m_info:
        print("Merchant Rafa_AI not found in merchant_data.json")
        return
    
    # Calculate Monthly Stats (Volume & Fraud) for Rule Matching
    # Volume
    monthly_vol = df['eur_amount'].sum()
    
    # Fraud (Volume based on manual definition: "ratio of fraudulent volume over total volume")
    fraud_vol = df[df['has_fraudulent_dispute']]['eur_amount'].sum()
    fraud_rate = (fraud_vol / monthly_vol) if monthly_vol > 0 else 0.0
    
    # Get Rule 17
    rule_17 = next((f for f in fees if f['ID'] == 17), None)
    if not rule_17:
        print("Rule 17 not found")
        return
        
    # Find Matching Transactions
    matching_amount = 0.0
    
    for _, tx in df.iterrows():
        # Determine Intracountry
        is_intra = (tx['issuing_country'] == tx['acquirer_country'])
        
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
        
        if match_fee_rule(ctx, rule_17):
            matching_amount += tx['eur_amount']
            
    # Calculate Delta
    # Delta = (New Rate - Old Rate) * Amount / 10000
    old_rate = rule_17['rate']
    new_rate = 1
    
    # Calculate delta with high precision
    delta = (new_rate - old_rate) * matching_amount / 10000.0
    
    print(f"{delta:.14f}")

if __name__ == "__main__":
    main()

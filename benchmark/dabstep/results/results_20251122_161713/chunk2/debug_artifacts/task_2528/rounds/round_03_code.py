# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2528
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 9175 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# --- Helper Functions ---
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if value is None: return None
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
        except:
            return None
    return None

def parse_range(range_str):
    """Parses strings like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if not isinstance(range_str, str):
        return None, None
    
    s = range_str.lower().strip()
    
    # Handle k/m suffixes
    def parse_val(x):
        x = x.strip()
        mult = 1
        if x.endswith('k'):
            mult = 1000
            x = x[:-1]
        elif x.endswith('m'):
            mult = 1000000
            x = x[:-1]
        elif x.endswith('%'):
            mult = 0.01
            x = x[:-1]
        try:
            return float(x) * mult
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        return parse_val(parts[0]), parse_val(parts[1])
    elif s.startswith('>'):
        val = parse_val(s[1:])
        return val, float('inf')
    elif s.startswith('<'):
        val = parse_val(s[1:])
        return float('-inf'), val
    elif s == 'immediate':
        return 0, 0
    return None, None

def is_in_range(value, range_str):
    """Checks if a numeric value fits in a range string."""
    if range_str is None: return True # Wildcard
    low, high = parse_range(range_str)
    if low is None: return False # Parsing failed
    
    s = range_str.lower().strip()
    if s.startswith('>'):
        return value > low
    if s.startswith('<'):
        return value < high
    
    return low <= value <= high

def match_fee_rule(tx_ctx, rule):
    """
    Matches a transaction context against a fee rule.
    tx_ctx: dict containing transaction and merchant details
    rule: dict representing a fee rule
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_ctx['card_scheme']:
        return False
        
    # 2. Account Type (List contains) - Wildcard allowed
    if rule.get('account_type'):
        if tx_ctx['account_type'] not in rule['account_type']:
            return False
            
    # 3. Capture Delay (Complex Logic)
    r_delay = rule.get('capture_delay')
    m_delay = str(tx_ctx['capture_delay'])
    if r_delay:
        if r_delay == 'manual':
            if m_delay != 'manual': return False
        elif r_delay == 'immediate':
            if m_delay != 'immediate': return False
        elif r_delay.startswith('<'):
            limit = float(r_delay[1:])
            if m_delay == 'immediate': pass 
            elif m_delay == 'manual': return False
            elif m_delay.replace('.','',1).isdigit():
                if not (float(m_delay) < limit): return False
            else: return False
        elif r_delay.startswith('>'):
            limit = float(r_delay[1:])
            if m_delay == 'manual': pass
            elif m_delay == 'immediate': return False
            elif m_delay.replace('.','',1).isdigit():
                if not (float(m_delay) > limit): return False
            else: return False
        elif '-' in r_delay:
            low, high = map(float, r_delay.split('-'))
            if m_delay.replace('.','',1).isdigit():
                val = float(m_delay)
                if not (low <= val <= high): return False
            else: return False
        else:
            if r_delay != m_delay: return False

    # 4. Merchant Category Code (List contains) - Wildcard allowed
    if rule.get('merchant_category_code'):
        if tx_ctx['mcc'] not in rule['merchant_category_code']:
            return False

    # 5. Monthly Volume (Range) - Wildcard allowed
    if rule.get('monthly_volume'):
        if not is_in_range(tx_ctx['monthly_volume'], rule['monthly_volume']):
            return False

    # 6. Monthly Fraud Level (Range) - Wildcard allowed
    if rule.get('monthly_fraud_level'):
        if not is_in_range(tx_ctx['monthly_fraud_rate'], rule['monthly_fraud_level']):
            return False

    # 7. Is Credit (Boolean) - Wildcard allowed
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_ctx['is_credit']:
            return False

    # 8. ACI (List contains) - Wildcard allowed
    if rule.get('aci'):
        if tx_ctx['aci'] not in rule['aci']:
            return False

    # 9. Intracountry (Boolean) - Wildcard allowed
    if rule.get('intracountry') is not None:
        rule_intra = bool(rule['intracountry'])
        if rule_intra != tx_ctx['intracountry']:
            return False

    return True

def calculate_fee(amount, rule):
    fixed = rule.get('fixed_amount', 0)
    rate = rule.get('rate', 0)
    return fixed + (rate * amount / 10000.0)

def execute_step():
    # Load Data
    payments_path = '/output/chunk2/data/context/payments.csv'
    fees_path = '/output/chunk2/data/context/fees.json'
    merchant_path = '/output/chunk2/data/context/merchant_data.json'
    
    try:
        payments = pd.read_csv(payments_path)
        with open(fees_path, 'r') as f:
            fees = json.load(f)
        with open(merchant_path, 'r') as f:
            merchant_data = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        return

    # Target Merchant
    merchant_name = 'Belles_cookbook_store'
    target_year = 2023
    
    # Get Merchant Attributes
    m_info = next((m for m in merchant_data if m['merchant'] == merchant_name), None)
    if not m_info:
        print("Merchant not found")
        return

    original_mcc = m_info['merchant_category_code']
    account_type = m_info['account_type']
    capture_delay = m_info['capture_delay']
    
    # Filter Transactions
    df = payments[(payments['merchant'] == merchant_name) & (payments['year'] == target_year)].copy()
    
    if df.empty:
        print("No transactions found for merchant in 2023.")
        return

    # Add Month (using day_of_year)
    # Simple mapping for non-leap year 2023
    def get_month(doy):
        if doy <= 31: return 1
        if doy <= 59: return 2
        if doy <= 90: return 3
        if doy <= 120: return 4
        if doy <= 151: return 5
        if doy <= 181: return 6
        if doy <= 212: return 7
        if doy <= 243: return 8
        if doy <= 273: return 9
        if doy <= 304: return 10
        if doy <= 334: return 11
        return 12
    
    df['month'] = df['day_of_year'].apply(get_month)
    
    # Calculate Monthly Stats (Volume and Fraud Rate)
    monthly_stats = {}
    for month in range(1, 13):
        m_df = df[df['month'] == month]
        if m_df.empty:
            monthly_stats[month] = {'vol': 0.0, 'fraud_rate': 0.0}
            continue
        
        vol = m_df['eur_amount'].sum()
        fraud_vol = m_df[m_df['has_fraudulent_dispute']]['eur_amount'].sum()
        # Manual: "ratio between monthly total volume and monthly volume notified as fraud"
        fraud_rate = fraud_vol / vol if vol > 0 else 0.0
        monthly_stats[month] = {'vol': vol, 'fraud_rate': fraud_rate}

    # Function to calculate total fees for a given MCC
    def get_total_fees(mcc_code):
        total_fees = 0.0
        
        # Iterate over transactions
        for _, tx in df.iterrows():
            # Build Context
            month = tx['month']
            stats = monthly_stats[month]
            
            ctx = {
                'card_scheme': tx['card_scheme'],
                'account_type': account_type,
                'capture_delay': capture_delay,
                'mcc': mcc_code,
                'monthly_volume': stats['vol'],
                'monthly_fraud_rate': stats['fraud_rate'],
                'is_credit': bool(tx['is_credit']),
                'aci': tx['aci'],
                'intracountry': tx['issuing_country'] == tx['acquirer_country'],
                'eur_amount': tx['eur_amount']
            }
            
            # Find Rule
            matched_rule = None
            for rule in fees:
                if match_fee_rule(ctx, rule):
                    matched_rule = rule
                    break
            
            if matched_rule:
                fee = calculate_fee(ctx['eur_amount'], matched_rule)
                total_fees += fee
                
        return total_fees

    # Calculate Fees
    fees_original = get_total_fees(original_mcc)
    fees_new = get_total_fees(8062)
    
    # Calculate Delta (New - Old)
    delta = fees_new - fees_original
    
    # Print result with high precision
    print(f"{delta:.14f}")

if __name__ == "__main__":
    execute_step()

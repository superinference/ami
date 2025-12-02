# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2397
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 9215 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# Helper functions for robust data processing
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

def safe_get(df, column, default=None):
    """Safely get column from DataFrame, return default if not exists."""
    if isinstance(df, dict):
        return df.get(column, default)
    elif hasattr(df, 'columns') and column in df.columns:
        return df[column]
    return default

def is_not_empty(array):
    """Check if array/list is not empty. Handles numpy arrays safely."""
    if array is None:
        return False
    if hasattr(array, 'size'):  # numpy array
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

# --- Custom Helpers for Fee Matching ---

def parse_vol_str(s):
    """Parse volume strings like '100k', '1m', '>10m'."""
    if not isinstance(s, str): return 0
    s = s.lower().replace(',', '').strip()
    s = s.replace('>', '').replace('<', '') # Remove operators for value parsing
    mult = 1
    if 'k' in s: mult = 1000; s = s.replace('k', '')
    if 'm' in s: mult = 1000000; s = s.replace('m', '')
    try:
        return float(s) * mult
    except:
        return 0

def check_range(val, rule_str, is_percent=False):
    """Check if value fits in rule range string (e.g. '100k-1m', '>5')."""
    if rule_str is None: return True
    s = str(rule_str).strip()
    
    if is_percent:
        # Convert val to percentage points (e.g. 0.083 -> 8.3) to match string "8.3%"
        val_to_compare = val * 100
        s = s.replace('%', '')
    else:
        val_to_compare = val

    # Handle ranges "min-max"
    if '-' in s:
        parts = s.split('-')
        try:
            low = parse_vol_str(parts[0])
            high = parse_vol_str(parts[1])
            return low <= val_to_compare <= high
        except:
            return False
            
    # Handle inequalities
    if s.startswith('>'):
        try:
            limit = parse_vol_str(s[1:])
            return val_to_compare > limit
        except:
            return False
    if s.startswith('<'):
        try:
            limit = parse_vol_str(s[1:])
            return val_to_compare < limit
        except:
            return False
            
    # Exact match fallback
    try:
        limit = parse_vol_str(s)
        return val_to_compare == limit
    except:
        return False

def check_capture_delay(merchant_val, rule_val):
    """Match merchant capture delay against rule."""
    if rule_val is None: return True
    
    # Direct string match
    if str(merchant_val) == str(rule_val): return True
    
    # Numeric logic for ranges like '<3', '3-5'
    # Merchant vals: '1', '2', '7', 'immediate', 'manual'
    try:
        days = int(merchant_val)
        if rule_val == '<3': return days < 3
        if rule_val == '3-5': return 3 <= days <= 5
        if rule_val == '>5': return days > 5
    except ValueError:
        # merchant_val is 'immediate' or 'manual', handled by direct match above
        pass
    return False

def match_fee_rule_internal(ctx, rule):
    """
    Check if a transaction context matches a fee rule.
    ctx: dict containing transaction and merchant details
    rule: dict containing fee rule details
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') != ctx.get('card_scheme'):
        return False
        
    # 2. Is Credit (Exact match, None=Wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != ctx.get('is_credit'):
            return False
            
    # 3. ACI (List match, Empty/None=Wildcard)
    if is_not_empty(rule.get('aci')):
        if ctx.get('aci') not in rule['aci']:
            return False
            
    # 4. Intracountry (Boolean match, None=Wildcard)
    if rule.get('intracountry') is not None:
        # Rule value might be string "0.0"/"1.0" or float
        rule_intra = float(rule['intracountry'])
        is_intra = 1.0 if (ctx.get('issuing_country') == ctx.get('acquirer_country')) else 0.0
        if rule_intra != is_intra:
            return False

    # 5. Account Type (List match, Empty/None=Wildcard)
    if is_not_empty(rule.get('account_type')):
        if ctx.get('account_type') not in rule['account_type']:
            return False
            
    # 6. MCC (List match, Empty/None=Wildcard)
    if is_not_empty(rule.get('merchant_category_code')):
        if ctx.get('mcc') not in rule['merchant_category_code']:
            return False
            
    # 7. Capture Delay (Custom logic)
    if not check_capture_delay(ctx.get('capture_delay'), rule.get('capture_delay')):
        return False
        
    # 8. Monthly Volume (Range match)
    if not check_range(ctx.get('monthly_volume'), rule.get('monthly_volume'), is_percent=False):
        return False
        
    # 9. Monthly Fraud Level (Range match)
    if not check_range(ctx.get('monthly_fraud_rate'), rule.get('monthly_fraud_level'), is_percent=True):
        return False
        
    return True

def execute_step():
    # 1. Load Data
    payments_path = '/output/chunk2/data/context/payments.csv'
    fees_path = '/output/chunk2/data/context/fees.json'
    merchant_path = '/output/chunk2/data/context/merchant_data.json'
    
    df_payments = pd.read_csv(payments_path)
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)

    # 2. Get Target Fee Rule (ID=787)
    target_rule = next((f for f in fees_data if f['ID'] == 787), None)
    if not target_rule:
        print("Fee ID 787 not found.")
        return

    # 3. Get Merchant Metadata for 'Rafa_AI'
    merchant_info = next((m for m in merchant_data if m['merchant'] == 'Rafa_AI'), None)
    if not merchant_info:
        print("Merchant Rafa_AI not found.")
        return

    # 4. Filter Transactions: Rafa_AI, May 2023 (Day 121-151)
    # Note: 2023 is not a leap year.
    # Jan=31, Feb=28, Mar=31, Apr=30 -> Sum=120. May 1st is Day 121.
    # May has 31 days -> Ends Day 151.
    rafa_may_txs = df_payments[
        (df_payments['merchant'] == 'Rafa_AI') & 
        (df_payments['year'] == 2023) & 
        (df_payments['day_of_year'] >= 121) & 
        (df_payments['day_of_year'] <= 151)
    ].copy()

    if rafa_may_txs.empty:
        print("No transactions found for Rafa_AI in May 2023.")
        return

    # 5. Calculate Monthly Stats (Volume & Fraud) for Rule Matching
    # Manual: "Monthly volumes and rates are computed always in natural months"
    # Manual: "monthly_fraud_level... ratio between monthly total volume and monthly volume notified as fraud"
    
    monthly_volume = rafa_may_txs['eur_amount'].sum()
    
    fraud_txs = rafa_may_txs[rafa_may_txs['has_fraudulent_dispute'] == True]
    fraud_volume = fraud_txs['eur_amount'].sum()
    
    monthly_fraud_rate = (fraud_volume / monthly_volume) if monthly_volume > 0 else 0.0

    # 6. Identify Matching Transactions
    matching_amounts = []
    
    # Pre-extract merchant static data
    m_account_type = merchant_info.get('account_type')
    m_mcc = merchant_info.get('merchant_category_code')
    m_capture_delay = merchant_info.get('capture_delay')

    for _, row in rafa_may_txs.iterrows():
        # Build context for this transaction
        ctx = {
            'card_scheme': row['card_scheme'],
            'is_credit': row['is_credit'],
            'aci': row['aci'],
            'issuing_country': row['issuing_country'],
            'acquirer_country': row['acquirer_country'],
            'eur_amount': row['eur_amount'],
            
            # Merchant static
            'account_type': m_account_type,
            'mcc': m_mcc,
            'capture_delay': m_capture_delay,
            
            # Monthly stats
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate
        }
        
        if match_fee_rule_internal(ctx, target_rule):
            matching_amounts.append(row['eur_amount'])

    # 7. Calculate Delta
    # Formula: fee = fixed + rate * amount / 10000
    # Delta = (New Fee - Old Fee)
    # Since fixed amount doesn't change, Delta = (New Rate - Old Rate) * Amount / 10000
    
    original_rate = target_rule['rate']
    new_rate = 1
    total_affected_amount = sum(matching_amounts)
    
    delta = (new_rate - original_rate) * total_affected_amount / 10000
    
    # Print result with high precision
    print(f"{delta:.14f}")

if __name__ == "__main__":
    execute_step()

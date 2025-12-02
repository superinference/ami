# ═══════════════════════════════════════════════════════════
# Round 3 - Task 1818
# ═══════════════════════════════════════════════════════════
# Temperature: 0.25
# Verification: sufficient
# Plan steps: 2
# Code length: 7966 characters (FULL CODE)
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
    return float(value)

def is_not_empty(array):
    """Check if array/list is not empty."""
    if array is None:
        return False
    if hasattr(array, 'size'):
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

def parse_range(value_str):
    """Parses range strings like '100k-1m', '<3', '>5', '7.7%-8.3%' into (min, max)."""
    if value_str is None:
        return None
    
    s = str(value_str).strip().lower().replace(',', '')
    
    # Handle percentages
    is_percent = '%' in s
    s = s.replace('%', '')
    
    # Handle k/m suffixes
    def parse_num(n_str):
        n_str = n_str.strip()
        mult = 1
        if n_str.endswith('k'):
            mult = 1000
            n_str = n_str[:-1]
        elif n_str.endswith('m'):
            mult = 1000000
            n_str = n_str[:-1]
        try:
            return float(n_str) * mult
        except:
            return 0.0

    if '-' in s:
        parts = s.split('-')
        low = parse_num(parts[0])
        high = parse_num(parts[1])
        if is_percent:
            low /= 100
            high /= 100
        return (low, high)
    
    if s.startswith('<'):
        val = parse_num(s[1:])
        if is_percent: val /= 100
        return (float('-inf'), val)
    
    if s.startswith('>'):
        val = parse_num(s[1:])
        if is_percent: val /= 100
        return (val, float('inf'))
        
    # Exact match treated as range [val, val]
    val = parse_num(s)
    if is_percent: val /= 100
    return (val, val)

def match_fee_rule(tx_context, rule):
    """
    Matches a transaction context against a fee rule.
    tx_context: dict containing transaction and merchant details
    rule: dict from fees.json
    """
    # 1. Card Scheme (Exact match)
    if rule['card_scheme'] != tx_context['card_scheme']:
        return False
        
    # 2. Account Type (List match - null/empty means all)
    if is_not_empty(rule['account_type']):
        if tx_context['account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match)
    if is_not_empty(rule['merchant_category_code']):
        if tx_context['mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Range/Value match)
    if rule['capture_delay']:
        merch_delay_str = str(tx_context['capture_delay'])
        
        # Handle specific string keywords in rule
        if rule['capture_delay'] == 'manual':
            if merch_delay_str != 'manual': return False
        elif rule['capture_delay'] == 'immediate':
            if merch_delay_str != 'immediate': return False
        else:
            # Convert merchant delay to numeric for range comparison
            if merch_delay_str == 'manual':
                merch_val = 999 # Treat manual as very long delay
            elif merch_delay_str == 'immediate':
                merch_val = 0
            else:
                try:
                    merch_val = float(merch_delay_str)
                except ValueError:
                    merch_val = 0 # Default fallback
            
            r_min, r_max = parse_range(rule['capture_delay'])
            if not (r_min <= merch_val <= r_max):
                return False

    # 5. Monthly Volume (Range match)
    if rule['monthly_volume']:
        r_min, r_max = parse_range(rule['monthly_volume'])
        if not (r_min <= tx_context['monthly_volume'] <= r_max):
            return False
            
    # 6. Monthly Fraud Level (Range match)
    if rule['monthly_fraud_level']:
        r_min, r_max = parse_range(rule['monthly_fraud_level'])
        # Fraud level is a ratio (0.0 - 1.0)
        if not (r_min <= tx_context['monthly_fraud_rate'] <= r_max):
            return False

    # 7. Is Credit (Boolean match)
    if rule['is_credit'] is not None:
        if rule['is_credit'] != tx_context['is_credit']:
            return False
            
    # 8. ACI (List match)
    if is_not_empty(rule['aci']):
        if tx_context['aci'] not in rule['aci']:
            return False
            
    # 9. Intracountry (Boolean match)
    if rule['intracountry'] is not None:
        is_intra = (tx_context['issuing_country'] == tx_context['acquirer_country'])
        if rule['intracountry'] != is_intra:
            return False
            
    return True

# --- Main Execution ---

def calculate_belles_fees():
    # 1. Load Data
    payments = pd.read_csv('/output/chunk4/data/context/payments.csv')
    with open('/output/chunk4/data/context/merchant_data.json') as f:
        merchant_data = json.load(f)
    with open('/output/chunk4/data/context/fees.json') as f:
        fees = json.load(f)
        
    target_merchant = 'Belles_cookbook_store'
    
    # 2. Filter for Merchant and August 2023
    # Create date column from year and day_of_year
    payments['date'] = pd.to_datetime(
        payments['year'].astype(str) + payments['day_of_year'].astype(str), 
        format='%Y%j'
    )
    
    # Filter mask
    mask = (payments['merchant'] == target_merchant) & \
           (payments['date'].dt.month == 8) & \
           (payments['date'].dt.year == 2023)
    
    txs = payments[mask].copy()
    
    if len(txs) == 0:
        print("0.00")
        return

    # 3. Get Merchant Metadata
    merch_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merch_info:
        print(f"Merchant data for {target_merchant} not found.")
        return
        
    # 4. Calculate Monthly Stats (Volume & Fraud)
    # Volume in Euros
    monthly_volume = txs['eur_amount'].sum()
    
    # Fraud Rate (Fraud Volume / Total Volume)
    fraud_vol = txs[txs['has_fraudulent_dispute']]['eur_amount'].sum()
    monthly_fraud_rate = fraud_vol / monthly_volume if monthly_volume > 0 else 0.0
    
    # 5. Calculate Fees per Transaction
    total_fees = 0.0
    
    for _, tx in txs.iterrows():
        # Build context for matching
        ctx = {
            'card_scheme': tx['card_scheme'],
            'account_type': merch_info['account_type'],
            'mcc': merch_info['merchant_category_code'],
            'capture_delay': merch_info['capture_delay'],
            'monthly_volume': monthly_volume,
            'monthly_fraud_rate': monthly_fraud_rate,
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'issuing_country': tx['issuing_country'],
            'acquirer_country': tx['acquirer_country'],
            'eur_amount': tx['eur_amount']
        }
        
        # Find first matching rule
        matched_rule = None
        for rule in fees:
            if match_fee_rule(ctx, rule):
                matched_rule = rule
                break 
        
        if matched_rule:
            # Fee = fixed_amount + (rate * amount / 10000)
            # rate is in basis points (per 10,000)
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000)
            total_fees += fee
            
    # Output just the number as requested
    print(f"{total_fees:.2f}")

if __name__ == "__main__":
    calculate_belles_fees()

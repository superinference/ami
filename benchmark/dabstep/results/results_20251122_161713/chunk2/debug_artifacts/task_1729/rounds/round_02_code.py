# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1729
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10662 characters (FULL CODE)
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

def parse_value_with_suffix(val_str):
    """Parse string with k/m suffixes to float."""
    if not isinstance(val_str, str):
        return float(val_str)
    val_str = val_str.lower().strip().replace(',', '').replace('€', '').replace('$', '')
    multiplier = 1
    if 'k' in val_str:
        multiplier = 1000
        val_str = val_str.replace('k', '')
    elif 'm' in val_str:
        multiplier = 1000000
        val_str = val_str.replace('m', '')
    
    if '%' in val_str:
        return (float(val_str.replace('%', '')) / 100) * multiplier
    return float(val_str) * multiplier

def check_range(rule_str, actual_value):
    """
    Check if actual_value falls within the range specified by rule_str.
    rule_str examples: '100k-1m', '>5', '<3', '7.7%-8.3%', 'immediate'
    """
    if rule_str is None:
        return True
    
    # Handle exact string matches for non-numeric rules (e.g. capture_delay)
    if isinstance(actual_value, str) and rule_str == actual_value:
        return True
        
    # If actual value is numeric, try to parse rule as range
    if isinstance(actual_value, (int, float)):
        s = str(rule_str).strip()
        
        # Handle ranges like "100k-1m" or "7.7%-8.3%"
        if '-' in s:
            parts = s.split('-')
            if len(parts) == 2:
                try:
                    min_val = parse_value_with_suffix(parts[0])
                    max_val = parse_value_with_suffix(parts[1])
                    return min_val <= actual_value <= max_val
                except ValueError:
                    pass
        
        # Handle inequalities like ">5", "<3"
        if s.startswith('>'):
            try:
                limit = parse_value_with_suffix(s[1:])
                return actual_value > limit
            except ValueError:
                pass
        if s.startswith('<'):
            try:
                limit = parse_value_with_suffix(s[1:])
                return actual_value < limit
            except ValueError:
                pass
                
        # Handle exact numeric match (rare in ranges but possible)
        try:
            val = parse_value_with_suffix(s)
            return actual_value == val
        except ValueError:
            pass
            
    return False

def match_fee_rule(transaction_context, rule):
    """
    Check if a fee rule applies to a transaction context.
    transaction_context must contain:
    - card_scheme, is_credit, aci, eur_amount, issuing_country, acquirer_country
    - merchant_account_type, merchant_mcc, merchant_capture_delay
    - monthly_volume, monthly_fraud_level
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') != transaction_context['card_scheme']:
        return False
        
    # 2. Account Type (List match or wildcard)
    if is_not_empty(rule.get('account_type')):
        if transaction_context['merchant_account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match or wildcard)
    if is_not_empty(rule.get('merchant_category_code')):
        if transaction_context['merchant_mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Exact/Range match or wildcard)
    if rule.get('capture_delay') is not None:
        # If merchant has specific string like "immediate", check equality
        # If merchant has numeric string "1", check range
        m_delay = transaction_context['merchant_capture_delay']
        r_delay = rule['capture_delay']
        
        # Try exact match first (handles "immediate", "manual")
        if m_delay == r_delay:
            pass
        # Try numeric range match if merchant delay is numeric
        elif str(m_delay).isdigit():
            if not check_range(r_delay, float(m_delay)):
                return False
        else:
            # Mismatch in categorical values
            return False

    # 5. Is Credit (Bool match or wildcard)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != transaction_context['is_credit']:
            return False
            
    # 6. ACI (List match or wildcard)
    if is_not_empty(rule.get('aci')):
        if transaction_context['aci'] not in rule['aci']:
            return False
            
    # 7. Intracountry (Bool match or wildcard)
    # Rule uses 1.0/0.0/None. Transaction is derived boolean.
    if rule.get('intracountry') is not None:
        is_intra = transaction_context['issuing_country'] == transaction_context['acquirer_country']
        rule_intra = bool(float(rule['intracountry'])) # Convert 1.0 -> True, 0.0 -> False
        if rule_intra != is_intra:
            return False
            
    # 8. Monthly Volume (Range match or wildcard)
    if rule.get('monthly_volume') is not None:
        if not check_range(rule['monthly_volume'], transaction_context['monthly_volume']):
            return False
            
    # 9. Monthly Fraud Level (Range match or wildcard)
    if rule.get('monthly_fraud_level') is not None:
        if not check_range(rule['monthly_fraud_level'], transaction_context['monthly_fraud_level']):
            return False
            
    return True

def calculate_total_fees():
    # File paths
    payments_path = '/output/chunk2/data/context/payments.csv'
    merchant_path = '/output/chunk2/data/context/merchant_data.json'
    fees_path = '/output/chunk2/data/context/fees.json'
    
    print("Loading data...")
    
    # Load Merchant Data
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
    
    # Load Fees Data
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
        
    # Load Payments Data
    # Optimization: Load only necessary columns if dataset is huge, but here we load all
    df = pd.read_csv(payments_path)
    
    target_merchant = "Martinis_Fine_Steakhouse"
    target_year = 2023
    target_day = 10
    
    # 1. Get Merchant Metadata
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Error: Merchant {target_merchant} not found in merchant_data.json")
        return

    print(f"Merchant Info: {merchant_info}")
    
    # 2. Calculate Monthly Stats for January 2023
    # Filter for Merchant + Year + Month (Jan = Days 1-31)
    jan_mask = (
        (df['merchant'] == target_merchant) & 
        (df['year'] == target_year) & 
        (df['day_of_year'] >= 1) & 
        (df['day_of_year'] <= 31)
    )
    jan_txs = df[jan_mask]
    
    if jan_txs.empty:
        print("Warning: No transactions found for January 2023. Using 0 volume/fraud.")
        monthly_volume = 0.0
        monthly_fraud_level = 0.0
    else:
        monthly_volume = jan_txs['eur_amount'].sum()
        fraud_volume = jan_txs[jan_txs['has_fraudulent_dispute']]['eur_amount'].sum()
        # Fraud level is ratio of fraud volume to total volume
        monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0
        
    print(f"January Stats - Volume: €{monthly_volume:,.2f}, Fraud Level: {monthly_fraud_level:.4%}")
    
    # 3. Filter Target Transactions (Day 10)
    target_mask = (
        (df['merchant'] == target_merchant) & 
        (df['year'] == target_year) & 
        (df['day_of_year'] == target_day)
    )
    target_txs = df[target_mask]
    
    print(f"Found {len(target_txs)} transactions for Day {target_day}.")
    
    # 4. Calculate Fees
    total_fees = 0.0
    
    # Pre-construct context parts that don't change per transaction
    base_context = {
        'merchant_account_type': merchant_info['account_type'],
        'merchant_mcc': merchant_info['merchant_category_code'],
        'merchant_capture_delay': merchant_info['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    for idx, tx in target_txs.iterrows():
        # Build full context for this transaction
        context = base_context.copy()
        context.update({
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'eur_amount': tx['eur_amount'],
            'issuing_country': tx['issuing_country'],
            'acquirer_country': tx['acquirer_country']
        })
        
        # Find matching rule
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(context, rule):
                matched_rule = rule
                break # Assume first match applies
        
        if matched_rule:
            # Calculate fee: fixed + (rate * amount / 10000)
            # Rate is typically in basis points * 100 (e.g. 19 -> 0.19%)? 
            # Manual says: "rate * transaction_value / 10000"
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000)
            total_fees += fee
        else:
            print(f"Warning: No fee rule found for transaction {tx['psp_reference']}")
            
    print(f"\nTotal Fees for {target_merchant} on Day {target_day}: €{total_fees:.2f}")
    print(f"{total_fees:.14f}") # High precision output

if __name__ == "__main__":
    calculate_total_fees()

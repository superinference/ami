import pandas as pd
import json
import numpy as np

# Helper functions for robust data processing
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
        except ValueError:
            return 0.0
    return float(value)

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
    
    try:
        return float(val_str) * multiplier
    except ValueError:
        return 0.0

def check_range(rule_str, actual_value):
    """
    Check if actual_value falls within the range specified by rule_str.
    """
    if rule_str is None:
        return True
    
    # Handle exact string matches for non-numeric rules (e.g. capture_delay="immediate")
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
                
        # Handle exact numeric match
        try:
            val = parse_value_with_suffix(s)
            # Use a small epsilon for float comparison if needed, or direct equality
            return actual_value == val
        except ValueError:
            pass
            
    return False

def is_not_empty(obj):
    """Check if list/array is not empty/None."""
    if obj is None:
        return False
    if isinstance(obj, (list, tuple, np.ndarray)):
        return len(obj) > 0
    return False

def match_fee_rule(transaction_context, rule):
    """
    Check if a fee rule applies to a transaction context.
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') != transaction_context['card_scheme']:
        return False
        
    # 2. Account Type (List match or wildcard)
    # Rule has list of allowed types. Merchant has one type.
    if is_not_empty(rule.get('account_type')):
        if transaction_context['merchant_account_type'] not in rule['account_type']:
            return False
            
    # 3. Merchant Category Code (List match or wildcard)
    if is_not_empty(rule.get('merchant_category_code')):
        if transaction_context['merchant_mcc'] not in rule['merchant_category_code']:
            return False
            
    # 4. Capture Delay (Exact/Range match or wildcard)
    if rule.get('capture_delay') is not None:
        m_delay = transaction_context['merchant_capture_delay']
        r_delay = rule['capture_delay']
        
        # If merchant delay is a number string (e.g. "1"), convert to float for range check
        # If it's "immediate" or "manual", keep as string
        try:
            m_delay_val = float(m_delay)
        except (ValueError, TypeError):
            m_delay_val = m_delay
            
        if not check_range(r_delay, m_delay_val):
            return False

    # 5. Is Credit (Bool match or wildcard)
    if rule.get('is_credit') is not None:
        # Ensure boolean comparison
        if bool(rule['is_credit']) != bool(transaction_context['is_credit']):
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
    
    # Load Data
    try:
        with open(merchant_path, 'r') as f:
            merchant_data = json.load(f)
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        df = pd.read_csv(payments_path)
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    target_merchant = "Martinis_Fine_Steakhouse"
    target_year = 2023
    target_day = 10
    
    # 1. Get Merchant Metadata
    merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
    if not merchant_info:
        print(f"Error: Merchant {target_merchant} not found")
        return

    # 2. Calculate Monthly Stats for January 2023 (Natural Month)
    # Filter for Merchant + Year + Month (Jan = Days 1-31)
    jan_mask = (
        (df['merchant'] == target_merchant) & 
        (df['year'] == target_year) & 
        (df['day_of_year'] >= 1) & 
        (df['day_of_year'] <= 31)
    )
    jan_txs = df[jan_mask]
    
    if jan_txs.empty:
        monthly_volume = 0.0
        monthly_fraud_level = 0.0
    else:
        monthly_volume = jan_txs['eur_amount'].sum()
        fraud_volume = jan_txs[jan_txs['has_fraudulent_dispute']]['eur_amount'].sum()
        monthly_fraud_level = fraud_volume / monthly_volume if monthly_volume > 0 else 0.0
        
    # 3. Filter Target Transactions (Day 10)
    target_mask = (
        (df['merchant'] == target_merchant) & 
        (df['year'] == target_year) & 
        (df['day_of_year'] == target_day)
    )
    target_txs = df[target_mask]
    
    # 4. Calculate Fees
    total_fees = 0.0
    
    base_context = {
        'merchant_account_type': merchant_info['account_type'],
        'merchant_mcc': merchant_info['merchant_category_code'],
        'merchant_capture_delay': merchant_info['capture_delay'],
        'monthly_volume': monthly_volume,
        'monthly_fraud_level': monthly_fraud_level
    }
    
    # Sort fees by ID to ensure deterministic matching order (though usually first match in list is standard)
    # Assuming fees_data is a list of rules in priority order or just a list.
    # If priority isn't specified, we take the first one that matches.
    fees_data.sort(key=lambda x: x['ID']) 

    for idx, tx in target_txs.iterrows():
        context = base_context.copy()
        context.update({
            'card_scheme': tx['card_scheme'],
            'is_credit': tx['is_credit'],
            'aci': tx['aci'],
            'eur_amount': tx['eur_amount'],
            'issuing_country': tx['issuing_country'],
            'acquirer_country': tx['acquirer_country']
        })
        
        matched_rule = None
        for rule in fees_data:
            if match_fee_rule(context, rule):
                matched_rule = rule
                break 
        
        if matched_rule:
            # Fee formula: fixed_amount + rate * transaction_value / 10000
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * tx['eur_amount'] / 10000)
            total_fees += fee
        else:
            # Fallback or error logging - strictly speaking, every tx should have a rule
            pass
            
    # Print result with high precision
    print(f"{total_fees:.14f}")

if __name__ == "__main__":
    calculate_total_fees()
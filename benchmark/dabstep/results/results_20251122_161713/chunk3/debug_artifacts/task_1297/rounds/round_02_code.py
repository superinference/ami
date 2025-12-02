# ═══════════════════════════════════════════════════════════
# Round 2 - Task 1297
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 10581 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import numpy as np
import json

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

def safe_index(array, idx, default=None):
    """Safely get array element at index, return default if out of bounds."""
    try:
        if 0 <= idx < len(array):
            return array[idx]
        return default
    except (IndexError, TypeError, AttributeError):
        return default

# --- Domain Specific Helpers ---

def get_month(day_of_year):
    """Convert day of year (1-365) to month (1-12). Assumes non-leap year (2023)."""
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    cumulative = 0
    for i, days in enumerate(days_in_months):
        cumulative += days
        if day_of_year <= cumulative:
            return i + 1
    return 12

def parse_volume_string(vol_str):
    """Parse volume range strings like '100k-1m' or '>10m'."""
    if not vol_str: return None
    v = vol_str.lower().replace(',', '')
    
    def parse_val(s):
        m = 1
        if 'k' in s: m = 1000; s = s.replace('k', '')
        elif 'm' in s: m = 1000000; s = s.replace('m', '')
        try:
            return float(s) * m
        except ValueError:
            return 0.0

    if '-' in v:
        try:
            low, high = v.split('-')
            return (parse_val(low), parse_val(high))
        except ValueError:
            return None
    elif '>' in v:
        return (parse_val(v.replace('>', '')), float('inf'))
    elif '<' in v:
        return (0, parse_val(v.replace('<', '')))
    return None

def check_volume_match(actual_vol, rule_vol_str):
    """Check if actual volume falls within the rule's volume range."""
    if not rule_vol_str: return True
    rng = parse_volume_string(rule_vol_str)
    if not rng: return True
    return rng[0] <= actual_vol <= rng[1]

def parse_fraud_string(fraud_str):
    """Parse fraud range strings like '7.7%-8.3%' or '>8.3%'."""
    if not fraud_str: return None
    v = fraud_str.replace('%', '')
    
    def parse_val(s):
        try:
            return float(s) / 100.0
        except ValueError:
            return 0.0

    if '-' in v:
        try:
            low, high = v.split('-')
            return (parse_val(low), parse_val(high))
        except ValueError:
            return None
    elif '>' in v:
        return (parse_val(v.replace('>', '')), float('inf'))
    elif '<' in v:
        return (0, parse_val(v.replace('<', '')))
    return None

def check_fraud_match(actual_rate, rule_fraud_str):
    """Check if actual fraud rate falls within the rule's fraud range."""
    if not rule_fraud_str: return True
    rng = parse_fraud_string(rule_fraud_str)
    if not rng: return True
    # Use a small epsilon for float comparison if needed, but direct comparison is usually sufficient
    return rng[0] <= actual_rate <= rng[1]

def check_capture_delay(merchant_delay, rule_delay):
    """Check if merchant capture delay matches the rule."""
    if not rule_delay: return True
    md = str(merchant_delay).lower()
    rd = str(rule_delay).lower()
    
    # Exact match (e.g., "manual" == "manual", "immediate" == "immediate")
    if md == rd: return True
    
    # Numeric range checks
    # Merchant delay might be "1", "2", "7"
    # Rule might be "<3", ">5", "3-5"
    if md.isdigit():
        md_val = float(md)
        if '-' in rd:
            try:
                low, high = map(float, rd.split('-'))
                return low <= md_val <= high
            except ValueError:
                pass
        elif '>' in rd:
            try:
                return md_val > float(rd.replace('>', ''))
            except ValueError:
                pass
        elif '<' in rd:
            try:
                return md_val < float(rd.replace('<', ''))
            except ValueError:
                pass
    
    return False

def solve():
    # 1. Load Data
    try:
        payments = pd.read_csv('/output/chunk3/data/context/payments.csv')
        with open('/output/chunk3/data/context/fees.json', 'r') as f:
            fees = json.load(f)
        with open('/output/chunk3/data/context/merchant_data.json', 'r') as f:
            merchant_data = json.load(f)
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # Convert merchant_data to dict for easy lookup
    merchants = {m['merchant']: m for m in merchant_data}

    # 2. Preprocessing Payments
    # Add Month
    payments['month'] = payments['day_of_year'].apply(get_month)
    
    # Calculate Monthly Stats per Merchant (Volume and Fraud Rate)
    # Fraud Rate = Fraud Volume / Total Volume (as per manual)
    payments['fraud_amount'] = np.where(payments['has_fraudulent_dispute'], payments['eur_amount'], 0.0)
    
    monthly_stats = payments.groupby(['merchant', 'month']).agg({
        'eur_amount': 'sum',
        'fraud_amount': 'sum'
    }).reset_index()
    
    monthly_stats.rename(columns={'eur_amount': 'monthly_volume'}, inplace=True)
    # Avoid division by zero
    monthly_stats['monthly_fraud_rate'] = monthly_stats.apply(
        lambda row: row['fraud_amount'] / row['monthly_volume'] if row['monthly_volume'] > 0 else 0.0, 
        axis=1
    )
    
    # Create a lookup for stats: (merchant, month) -> (vol, fraud_rate)
    stats_lookup = {}
    for _, row in monthly_stats.iterrows():
        stats_lookup[(row['merchant'], row['month'])] = (row['monthly_volume'], row['monthly_fraud_rate'])

    # 3. Filter Target Transactions
    # Question: "For credit transactions... GlobalCard... average fee... for 1234 EUR"
    # We simulate the fee for 1234 EUR on every historical GlobalCard Credit transaction
    target_txs = payments[
        (payments['card_scheme'] == 'GlobalCard') & 
        (payments['is_credit'] == True)
    ].copy()
    
    if target_txs.empty:
        print("No matching transactions found.")
        return

    # 4. Calculate Fees
    calculated_fees = []
    
    # Pre-filter fees for GlobalCard to optimize matching loop
    global_fees = [f for f in fees if f['card_scheme'] == 'GlobalCard']
    
    # Sort fees by ID to ensure deterministic matching (though usually first match is standard)
    global_fees.sort(key=lambda x: x['ID'])

    for _, tx in target_txs.iterrows():
        merch_name = tx['merchant']
        if merch_name not in merchants:
            continue 
            
        merch_info = merchants[merch_name]
        month = tx['month']
        
        # Retrieve Context Variables
        vol, fraud_rate = stats_lookup.get((merch_name, month), (0.0, 0.0))
        is_intracountry = (tx['issuing_country'] == tx['acquirer_country'])
        
        # Find matching rule
        matched_rule = None
        for rule in global_fees:
            # 1. Check is_credit (Rule: True or Null. Tx: True)
            # If rule is explicitly False, it doesn't match our Credit tx
            if rule['is_credit'] is not None and rule['is_credit'] is False:
                continue
                
            # 2. Check Account Type (Rule: List or Null. Tx: Single Value)
            if rule['account_type'] and merch_info['account_type'] not in rule['account_type']:
                continue
                
            # 3. Check MCC (Rule: List or Null. Tx: Single Value)
            if rule['merchant_category_code'] and merch_info['merchant_category_code'] not in rule['merchant_category_code']:
                continue
                
            # 4. Check ACI (Rule: List or Null. Tx: Single Value)
            if rule['aci'] and tx['aci'] not in rule['aci']:
                continue
                
            # 5. Check Intracountry (Rule: 0.0/1.0 or Null. Tx: Bool)
            if rule['intracountry'] is not None:
                rule_intra = bool(rule['intracountry'])
                if rule_intra != is_intracountry:
                    continue
            
            # 6. Check Capture Delay
            if not check_capture_delay(merch_info['capture_delay'], rule['capture_delay']):
                continue
                
            # 7. Check Monthly Volume
            if not check_volume_match(vol, rule['monthly_volume']):
                continue
                
            # 8. Check Monthly Fraud Level
            if not check_fraud_match(fraud_rate, rule['monthly_fraud_level']):
                continue
            
            # If all conditions pass, this is our rule
            matched_rule = rule
            break
        
        if matched_rule:
            # Calculate Fee for hypothetical amount of 1234 EUR
            # Formula: fee = fixed_amount + (rate * amount / 10000)
            # rate is in basis points (per 10,000)
            hypothetical_amount = 1234.0
            fee = matched_rule['fixed_amount'] + (matched_rule['rate'] * hypothetical_amount / 10000.0)
            calculated_fees.append(fee)
        else:
            # If no rule matches, we skip. In a real audit, this would be flagged.
            pass

    # 5. Average and Output
    if not calculated_fees:
        print("No applicable fees found.")
    else:
        avg_fee = sum(calculated_fees) / len(calculated_fees)
        # Print with high precision as requested for calculation tasks
        print(f"{avg_fee:.14f}")

if __name__ == "__main__":
    solve()

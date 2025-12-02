import pandas as pd
import json
import numpy as np

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if pd.isna(value) or value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
        # Range handling (e.g., "50-60") - return mean
        if '-' in v and len(v.split('-')) == 2:
            try:
                parts = v.split('-')
                # Handle k/m suffixes in ranges like "100k-1m"
                def parse_suffix(s):
                    s = s.lower()
                    if 'k' in s: return float(s.replace('k', '')) * 1000
                    if 'm' in s: return float(s.replace('m', '')) * 1000000
                    return float(s)
                return (parse_suffix(parts[0]) + parse_suffix(parts[1])) / 2
            except:
                pass
        # Handle single values with suffixes
        v_lower = v.lower()
        if 'k' in v_lower: return float(v_lower.replace('k', '')) * 1000
        if 'm' in v_lower: return float(v_lower.replace('m', '')) * 1000000
        try:
            return float(v)
        except ValueError:
            return 0.0
    return float(value)

def parse_range_check(value, rule_value):
    """
    Checks if a value falls within a rule's range string (e.g., '100k-1m', '>5', '7.7%-8.3%').
    Returns True if match, False otherwise.
    """
    if rule_value is None:
        return True
        
    try:
        # Handle percentage ranges
        is_percent = '%' in str(rule_value)
        
        # Clean up rule string
        rule_str = str(rule_value).lower().replace('%', '').replace(',', '')
        
        # Handle simple operators
        if rule_str.startswith('>'):
            limit = float(rule_str[1:])
            if is_percent: limit /= 100
            return value > limit
        if rule_str.startswith('<'):
            limit = float(rule_str[1:])
            if is_percent: limit /= 100
            return value < limit
            
        # Handle ranges "min-max"
        if '-' in rule_str:
            parts = rule_str.split('-')
            if len(parts) == 2:
                def parse_part(p):
                    if 'k' in p: return float(p.replace('k', '')) * 1000
                    if 'm' in p: return float(p.replace('m', '')) * 1000000
                    return float(p)
                
                min_val = parse_part(parts[0])
                max_val = parse_part(parts[1])
                
                if is_percent:
                    min_val /= 100
                    max_val /= 100
                    
                return min_val <= value <= max_val
                
        # Exact match (fallback)
        return value == float(rule_str)
    except:
        return False

def match_fee_rule(tx_context, rule):
    """
    Determines if a fee rule applies to a transaction context.
    tx_context must contain: 
      card_scheme, account_type, merchant_category_code, is_credit, aci, 
      monthly_volume, monthly_fraud_level, capture_delay, intracountry
    """
    # 1. Card Scheme (Exact match)
    if rule.get('card_scheme') and rule['card_scheme'] != tx_context.get('card_scheme'):
        return False

    # 2. Account Type (List match)
    if rule.get('account_type'):
        # If rule has list, merchant's type must be in it
        if tx_context.get('account_type') not in rule['account_type']:
            return False

    # 3. Merchant Category Code (List match)
    if rule.get('merchant_category_code'):
        if tx_context.get('merchant_category_code') not in rule['merchant_category_code']:
            return False

    # 4. Is Credit (Boolean match)
    if rule.get('is_credit') is not None:
        if rule['is_credit'] != tx_context.get('is_credit'):
            return False

    # 5. ACI (List match)
    if rule.get('aci'):
        if tx_context.get('aci') not in rule['aci']:
            return False

    # 6. Intracountry (Boolean match)
    if rule.get('intracountry') is not None:
        # Intracountry is True if issuer == acquirer
        is_intra = tx_context.get('issuing_country') == tx_context.get('acquirer_country')
        # Rule expects boolean or string '0.0'/'1.0'
        rule_intra = rule['intracountry']
        if isinstance(rule_intra, str):
            rule_intra = (float(rule_intra) == 1.0)
        if rule_intra != is_intra:
            return False

    # 7. Monthly Volume (Range match)
    if rule.get('monthly_volume'):
        if not parse_range_check(tx_context.get('monthly_volume', 0), rule['monthly_volume']):
            return False

    # 8. Monthly Fraud Level (Range match)
    if rule.get('monthly_fraud_level'):
        if not parse_range_check(tx_context.get('monthly_fraud_level', 0), rule['monthly_fraud_level']):
            return False

    # 9. Capture Delay (Match)
    if rule.get('capture_delay'):
        # Rule might be range or exact string
        # Merchant data has specific value e.g. "manual", "immediate", "1"
        # Simple string match or range check if numeric
        m_delay = str(tx_context.get('capture_delay', ''))
        r_delay = str(rule['capture_delay'])
        
        # Handle numeric comparison if both look numeric
        if m_delay.isdigit() and (r_delay.startswith('>') or r_delay.startswith('<')):
            if not parse_range_check(float(m_delay), r_delay):
                return False
        elif m_delay != r_delay:
            # If not numeric range, expect exact string match (e.g. "manual")
            return False

    return True

# ==========================================
# MAIN SCRIPT
# ==========================================

# File paths
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_path = '/output/chunk5/data/context/merchant_data.json'

# 1. Load Data
print("Loading data...")
df_payments = pd.read_csv(payments_path)
with open(fees_path, 'r') as f:
    fees_data = json.load(f)
with open(merchant_path, 'r') as f:
    merchant_data = json.load(f)

# 2. Setup Target
target_merchant = 'Golfclub_Baron_Friso'
target_year = 2023
target_fee_id = 65
new_rate_value = 1

# 3. Get Merchant Metadata
merchant_info = next((m for m in merchant_data if m['merchant'] == target_merchant), None)
if not merchant_info:
    raise ValueError(f"Merchant {target_merchant} not found in merchant_data.json")

print(f"Merchant Info: {merchant_info}")

# 4. Get Target Fee Rule
fee_rule_65 = next((f for f in fees_data if f['ID'] == target_fee_id), None)
if not fee_rule_65:
    raise ValueError(f"Fee rule ID {target_fee_id} not found in fees.json")

print(f"Fee Rule 65: {fee_rule_65}")
old_rate = fee_rule_65['rate']
print(f"Old Rate: {old_rate}, New Rate: {new_rate_value}")

# 5. Filter Transactions
df_filtered = df_payments[
    (df_payments['merchant'] == target_merchant) & 
    (df_payments['year'] == target_year)
].copy()

print(f"Found {len(df_filtered)} transactions for {target_merchant} in {target_year}")

# 6. Calculate Monthly Stats (Volume & Fraud)
# Manual says: "Monthly volumes and rates are computed always in natural months"
# We need these to check if the fee rule applies (if it has volume/fraud constraints)
df_filtered['month'] = pd.to_datetime(df_filtered['day_of_year'], unit='D', origin=f'{target_year}-01-01').dt.month

monthly_stats = {}
for month in df_filtered['month'].unique():
    month_txs = df_filtered[df_filtered['month'] == month]
    total_vol = month_txs['eur_amount'].sum()
    fraud_vol = month_txs[month_txs['has_fraudulent_dispute']]['eur_amount'].sum() # Fraud level is usually fraud_vol/total_vol or count/count. 
    # Manual says: "fraud levels measured as ratio between monthly total volume and monthly volume notified as fraud"
    # Wait, usually it's fraud_volume / total_volume. Let's re-read manual carefully.
    # Manual: "ratio between monthly total volume and monthly volume notified as fraud" -> This phrasing is ambiguous. 
    # Usually it is (Fraud Volume / Total Volume). 
    # Let's assume Fraud Volume / Total Volume.
    
    fraud_ratio = (fraud_vol / total_vol) if total_vol > 0 else 0.0
    monthly_stats[month] = {
        'volume': total_vol,
        'fraud_level': fraud_ratio
    }

# 7. Calculate Delta
# Iterate through transactions, check if fee rule 65 applies, calculate delta
total_delta = 0.0
matching_tx_count = 0

for idx, row in df_filtered.iterrows():
    # Build transaction context
    month = row['month']
    stats = monthly_stats.get(month, {'volume': 0, 'fraud_level': 0})
    
    tx_context = {
        'card_scheme': row['card_scheme'],
        'account_type': merchant_info['account_type'],
        'merchant_category_code': merchant_info['merchant_category_code'],
        'is_credit': row['is_credit'],
        'aci': row['aci'],
        'issuing_country': row['issuing_country'],
        'acquirer_country': row['acquirer_country'],
        'monthly_volume': stats['volume'],
        'monthly_fraud_level': stats['fraud_level'],
        'capture_delay': merchant_info['capture_delay']
    }
    
    # Check if fee rule 65 applies
    if match_fee_rule(tx_context, fee_rule_65):
        matching_tx_count += 1
        amount = row['eur_amount']
        
        # Calculate fee components
        # Fee = Fixed + (Rate * Amount / 10000)
        # Delta = New_Fee - Old_Fee
        # Delta = (Fixed + New_Rate * Amt / 10000) - (Fixed + Old_Rate * Amt / 10000)
        # Delta = (New_Rate - Old_Rate) * Amt / 10000
        
        delta = (new_rate_value - old_rate) * amount / 10000.0
        total_delta += delta

print(f"Matching Transactions: {matching_tx_count}")
print(f"Total Delta: {total_delta:.14f}")
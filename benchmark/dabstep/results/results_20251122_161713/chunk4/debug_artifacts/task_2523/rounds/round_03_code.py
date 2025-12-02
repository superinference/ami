# ═══════════════════════════════════════════════════════════
# Round 3 - Task 2523
# ═══════════════════════════════════════════════════════════
# Temperature: 0.20
# Verification: sufficient
# Plan steps: 3
# Code length: 8516 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import json
import pandas as pd
import numpy as np

# --- Helper Functions ---

def coerce_to_float(value):
    """Convert string with %, $, k, m, commas to float. Handles common formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators for conversion
        
        # Handle Percentages
        if '%' in v:
            try:
                return float(v.replace('%', '')) / 100
            except ValueError:
                return 0.0
            
        # Handle k/m suffixes (thousands/millions)
        lower_v = v.lower()
        if 'k' in lower_v:
            try:
                return float(lower_v.replace('k', '')) * 1000
            except ValueError:
                pass
        if 'm' in lower_v:
            try:
                return float(lower_v.replace('m', '')) * 1000000
            except ValueError:
                pass
            
        # Handle Ranges (return mean) - fallback if used directly
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

def parse_range(range_str):
    """Parses a range string like '100k-1m', '>5', or '<3' into (min, max)."""
    if not range_str:
        return -float('inf'), float('inf')
    
    s = str(range_str).lower().strip()
    
    # Handle > (Greater than)
    if s.startswith('>'):
        val = coerce_to_float(s[1:])
        return val, float('inf') # Treat > as strictly greater in check_condition
        
    # Handle < (Less than)
    if s.startswith('<'):
        val = coerce_to_float(s[1:])
        return -float('inf'), val
        
    # Handle Range (e.g., 100k-1m)
    if '-' in s:
        parts = s.split('-')
        min_val = coerce_to_float(parts[0])
        max_val = coerce_to_float(parts[1])
        return min_val, max_val
    
    # Exact match (treated as min=max)
    val = coerce_to_float(s)
    return val, val

def check_condition(value, condition_str):
    """Checks if a numeric value meets a condition string."""
    if condition_str is None:
        return True
        
    min_v, max_v = parse_range(condition_str)
    
    # Handle strict inequalities based on string presence
    s = str(condition_str).strip()
    if s.startswith('>'):
        return value > min_v
    if s.startswith('<'):
        return value < max_v
        
    # Default to inclusive range
    return min_v <= value <= max_v

# --- Main Analysis ---

def main():
    # 1. Load Data
    fees_path = '/output/chunk4/data/context/fees.json'
    payments_path = '/output/chunk4/data/context/payments.csv'
    merchant_path = '/output/chunk4/data/context/merchant_data.json'

    try:
        with open(fees_path, 'r') as f:
            fees_data = json.load(f)
        
        merchant_data = pd.read_json(merchant_path)
        payments = pd.read_csv(payments_path)
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # 2. Get Fee Rule 12
    fee_rule = next((f for f in fees_data if f['ID'] == 12), None)
    if not fee_rule:
        print("Fee ID 12 not found")
        return

    # Debug: Print rule details to ensure we understand criteria
    # print(f"DEBUG: Analyzing Fee Rule 12: {fee_rule}")

    # 3. Filter Payments for 2023
    df_2023 = payments[payments['year'] == 2023].copy()

    # 4. Merge Merchant Data
    # Add static merchant attributes (MCC, Account Type, Capture Delay) to transactions
    # Ensure column names match: merchant_data has 'merchant', 'merchant_category_code', 'account_type', 'capture_delay'
    df_merged = df_2023.merge(
        merchant_data[['merchant', 'merchant_category_code', 'account_type', 'capture_delay']], 
        on='merchant', 
        how='left'
    )

    # 5. Apply Static Filters from Fee Rule
    
    # Filter: Card Scheme
    if fee_rule.get('card_scheme'):
        df_merged = df_merged[df_merged['card_scheme'] == fee_rule['card_scheme']]

    # Filter: Is Credit (Boolean or Null)
    if fee_rule.get('is_credit') is not None:
        df_merged = df_merged[df_merged['is_credit'] == fee_rule['is_credit']]

    # Filter: ACI (List of strings)
    # If fee_rule['aci'] is empty list or None, it applies to all.
    if fee_rule.get('aci'): 
        df_merged = df_merged[df_merged['aci'].isin(fee_rule['aci'])]

    # Filter: Merchant Category Code (List of ints)
    if fee_rule.get('merchant_category_code'):
        df_merged = df_merged[df_merged['merchant_category_code'].isin(fee_rule['merchant_category_code'])]

    # Filter: Account Type (List of strings)
    if fee_rule.get('account_type'):
        df_merged = df_merged[df_merged['account_type'].isin(fee_rule['account_type'])]

    # Filter: Intracountry (Boolean or Null)
    if fee_rule.get('intracountry') is not None:
        # Intracountry means issuing_country == acquirer_country
        is_intra = df_merged['issuing_country'] == df_merged['acquirer_country']
        if fee_rule['intracountry']:
            df_merged = df_merged[is_intra]
        else:
            df_merged = df_merged[~is_intra]

    # Filter: Capture Delay (String)
    if fee_rule.get('capture_delay'):
        # Exact match for categorical strings
        df_merged = df_merged[df_merged['capture_delay'] == fee_rule['capture_delay']]

    # 6. Dynamic Filters (Volume/Fraud)
    # These require calculating stats on the TOTAL volume for the merchant/month.
    
    has_vol_rule = fee_rule.get('monthly_volume') is not None
    has_fraud_rule = fee_rule.get('monthly_fraud_level') is not None

    if has_vol_rule or has_fraud_rule:
        # We need to check if the merchant met the volume/fraud criteria in the specific month 
        # where the transaction occurred.
        
        # Add month column to filtered transactions
        df_merged['month'] = pd.to_datetime(df_merged['day_of_year'], unit='D', origin='2022-12-31').dt.month
        
        # Get unique (merchant, month) pairs that are candidates
        candidates = df_merged[['merchant', 'month']].drop_duplicates()
        
        if candidates.empty:
            print("") # No candidates left
            return

        # Calculate stats using the FULL 2023 dataset for these merchant/months
        full_2023 = payments[payments['year'] == 2023].copy()
        full_2023['month'] = pd.to_datetime(full_2023['day_of_year'], unit='D', origin='2022-12-31').dt.month
        
        # Filter full data to only relevant merchants to speed up grouping
        relevant_merchants = candidates['merchant'].unique()
        full_2023 = full_2023[full_2023['merchant'].isin(relevant_merchants)]
        
        # Aggregate monthly stats
        stats = full_2023.groupby(['merchant', 'month']).agg(
            monthly_vol=('eur_amount', 'sum'),
            fraud_vol=('eur_amount', lambda x: x[full_2023.loc[x.index, 'has_fraudulent_dispute']].sum())
        ).reset_index()
        
        stats['monthly_fraud_rate'] = stats['fraud_vol'] / stats['monthly_vol']
        
        # Identify valid (merchant, month) pairs that meet the rule
        valid_pairs = set()
        for _, row in stats.iterrows():
            vol_ok = True
            fraud_ok = True
            
            if has_vol_rule:
                vol_ok = check_condition(row['monthly_vol'], fee_rule['monthly_volume'])
                
            if has_fraud_rule:
                fraud_ok = check_condition(row['monthly_fraud_rate'], fee_rule['monthly_fraud_level'])
                
            if vol_ok and fraud_ok:
                valid_pairs.add((row['merchant'], row['month']))
        
        # Filter our potential matches to only include those in valid months
        # Create a tuple column for fast filtering
        df_merged['merchant_month'] = list(zip(df_merged['merchant'], df_merged['month']))
        df_merged = df_merged[df_merged['merchant_month'].isin(valid_pairs)]

    # 7. Output Result
    affected_merchants = sorted(df_merged['merchant'].unique())
    
    # Format output as requested (comma separated list)
    print(", ".join(affected_merchants))

if __name__ == "__main__":
    main()

# ═══════════════════════════════════════════════════════════
# Round 2 - Task 2522
# ═══════════════════════════════════════════════════════════
# Temperature: 0.15
# Verification: insufficient
# Plan steps: 2
# Code length: 6631 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

import pandas as pd
import json
import numpy as np

# Helper functions
def coerce_to_float(value):
    """Convert string with %, $, commas to float. Handles common formats."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip().replace(',', '').replace('€', '').replace('$', '')
        v = v.lstrip('><≤≥')  # Remove comparison operators
        if '%' in v:
            return float(v.replace('%', '')) / 100
        if 'm' in v.lower():
            return float(v.lower().replace('m', '')) * 1_000_000
        if 'k' in v.lower():
            return float(v.lower().replace('k', '')) * 1_000
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
    return float(value) if value is not None else 0.0

def safe_get(df, column, default=None):
    if isinstance(df, dict):
        return df.get(column, default)
    elif hasattr(df, 'columns') and column in df.columns:
        return df[column]
    return default

def is_not_empty(array):
    if array is None:
        return False
    if hasattr(array, 'size'):
        return array.size > 0
    try:
        return len(array) > 0
    except TypeError:
        return False

# Load data
try:
    fees_path = '/output/chunk3/data/context/fees.json'
    merchant_path = '/output/chunk3/data/context/merchant_data.json'
    payments_path = '/output/chunk3/data/context/payments.csv'

    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    
    with open(merchant_path, 'r') as f:
        merchant_data = json.load(f)
        
    df_payments = pd.read_csv(payments_path)
    df_merchants = pd.DataFrame(merchant_data)

    # 1. Get Fee ID 10 Criteria
    fee_rule = next((f for f in fees_data if f['ID'] == 10), None)
    if not fee_rule:
        print("Fee ID 10 not found.")
        exit()

    # Extract criteria
    target_mccs = fee_rule.get('merchant_category_code') # List
    target_capture_delay = fee_rule.get('capture_delay') # String
    target_scheme = fee_rule.get('card_scheme') # String
    target_aci = fee_rule.get('aci') # List
    target_intracountry = fee_rule.get('intracountry') # Bool
    target_volume_str = fee_rule.get('monthly_volume') # String ">5m"
    
    # Parse volume threshold
    volume_threshold = 0
    if target_volume_str:
        volume_threshold = coerce_to_float(target_volume_str)
    
    print(f"Analyzing Fee ID 10:")
    print(f" - MCCs: {target_mccs}")
    print(f" - Capture Delay: {target_capture_delay}")
    print(f" - Scheme: {target_scheme}")
    print(f" - ACI: {target_aci}")
    print(f" - Intracountry: {target_intracountry}")
    print(f" - Volume Threshold: > {volume_threshold:,.0f}")

    # 2. Filter Merchants by Static Data (MCC, Capture Delay)
    # Note: account_type is [] (wildcard) for Fee 10, so we skip it
    
    # Filter merchant_data
    candidate_merchants_df = df_merchants.copy()
    
    # Filter by MCC
    if target_mccs:
        candidate_merchants_df = candidate_merchants_df[candidate_merchants_df['merchant_category_code'].isin(target_mccs)]
    
    # Filter by Capture Delay
    if target_capture_delay:
        candidate_merchants_df = candidate_merchants_df[candidate_merchants_df['capture_delay'] == target_capture_delay]
        
    candidate_merchant_names = candidate_merchants_df['merchant'].unique().tolist()
    print(f"\nMerchants matching static criteria (MCC, Capture Delay): {candidate_merchant_names}")

    if not candidate_merchant_names:
        print("No merchants match the static criteria.")
        exit()

    # 3. Filter Transactions by Dynamic Data
    # Filter payments for candidate merchants
    df_filtered = df_payments[df_payments['merchant'].isin(candidate_merchant_names)].copy()
    
    # Add Month column (Year is 2023)
    # Create a date column to extract month
    df_filtered['date'] = pd.to_datetime(df_filtered['year'] * 1000 + df_filtered['day_of_year'], format='%Y%j')
    df_filtered['month'] = df_filtered['date'].dt.month

    # 4. Calculate Monthly Volume per Merchant
    # Group by merchant and month to get total volume
    monthly_volumes = df_filtered.groupby(['merchant', 'month'])['eur_amount'].sum().reset_index()
    
    # Identify (Merchant, Month) tuples that meet the volume threshold
    high_volume_months = monthly_volumes[monthly_volumes['eur_amount'] > volume_threshold]
    
    # Create a set of valid (merchant, month) keys
    valid_merchant_months = set(zip(high_volume_months['merchant'], high_volume_months['month']))
    
    print(f"Found {len(valid_merchant_months)} merchant-month combinations exceeding volume threshold.")

    # 5. Check Transaction-Level Criteria in Valid Months
    # Filter original transactions to those in valid months
    # We can do this by filtering the df_filtered
    
    # Helper to check if row is in valid months
    def is_valid_month(row):
        return (row['merchant'], row['month']) in valid_merchant_months

    # Apply volume filter to transactions
    # (Only transactions in high-volume months are eligible for this fee)
    df_eligible_volume = df_filtered[df_filtered.apply(is_valid_month, axis=1)]
    
    # Apply specific fee criteria
    # Scheme
    if target_scheme:
        df_eligible_volume = df_eligible_volume[df_eligible_volume['card_scheme'] == target_scheme]
    
    # ACI
    if target_aci:
        df_eligible_volume = df_eligible_volume[df_eligible_volume['aci'].isin(target_aci)]
        
    # Intracountry
    # True = Domestic (Issuer == Acquirer), False = International (Issuer != Acquirer)
    if target_intracountry is not None:
        if target_intracountry: # Must be domestic
            df_eligible_volume = df_eligible_volume[df_eligible_volume['issuing_country'] == df_eligible_volume['acquirer_country']]
        else: # Must be international
            df_eligible_volume = df_eligible_volume[df_eligible_volume['issuing_country'] != df_eligible_volume['acquirer_country']]

    # 6. Get Final Affected Merchants
    affected_merchants = df_eligible_volume['merchant'].unique().tolist()
    
    print("\nAffected Merchants:")
    if affected_merchants:
        print(", ".join(sorted(affected_merchants)))
    else:
        print("None")

except Exception as e:
    print(f"An error occurred: {e}")
    import traceback
    traceback.print_exc()

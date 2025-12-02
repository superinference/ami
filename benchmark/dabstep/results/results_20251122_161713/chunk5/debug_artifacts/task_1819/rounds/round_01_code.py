# ═══════════════════════════════════════════════════════════
# Round 1 - Task 1819
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3541 characters (FULL CODE)
# ───────────────────────────────────────────────────────────

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


import pandas as pd
import json

# Define file paths
payments_path = '/output/chunk5/data/context/payments.csv'
fees_path = '/output/chunk5/data/context/fees.json'
merchant_data_path = '/output/chunk5/data/context/merchant_data.json'
acquirer_countries_path = '/output/chunk5/data/context/acquirer_countries.csv'

# Load the datasets
try:
    # Load payments.csv
    df_payments = pd.read_csv(payments_path)
    print(f"Successfully loaded payments.csv. Shape: {df_payments.shape}")
    
    # Load fees.json
    # Loading as a list of dicts first is often safer for complex nested structures, 
    # then converting to DataFrame for display/filtering if needed.
    with open(fees_path, 'r') as f:
        fees_data = json.load(f)
    df_fees = pd.DataFrame(fees_data)
    print(f"Successfully loaded fees.json. Shape: {df_fees.shape}")
    
    # Load merchant_data.json
    df_merchant_data = pd.read_json(merchant_data_path)
    print(f"Successfully loaded merchant_data.json. Shape: {df_merchant_data.shape}")
    
    # Load acquirer_countries.csv
    df_acquirer_countries = pd.read_csv(acquirer_countries_path)
    print(f"Successfully loaded acquirer_countries.csv. Shape: {df_acquirer_countries.shape}")

    # Display a preview of the payments data to verify columns (especially 'merchant', 'year', 'day_of_year', 'eur_amount')
    print("\n--- Payments Data Preview ---")
    print(df_payments[['merchant', 'year', 'day_of_year', 'eur_amount', 'acquirer_country']].head())

    # Display a preview of the fees data to verify structure (nested lists like 'merchant_category_code', 'aci')
    print("\n--- Fees Data Preview ---")
    print(df_fees[['card_scheme', 'merchant_category_code', 'aci', 'fixed_amount', 'rate']].head())

except FileNotFoundError as e:
    print(f"Error: File not found - {e}")
except Exception as e:
    print(f"An error occurred during data loading: {e}")

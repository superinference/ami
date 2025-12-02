import pandas as pd
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

def safe_index(array, idx, default=None):
    """Safely get array element at index, return default if out of bounds."""
    try:
        if 0 <= idx < len(array):
            return array[idx]
        return default
    except (IndexError, TypeError, AttributeError):
        return default

# Main Analysis Script
if __name__ == "__main__":
    # Define the file path
    file_path = '/output/chunk1/data/context/payments.csv'

    # Load the 'payments.csv' file into a dataframe
    try:
        df = pd.read_csv(file_path)
        print(f"Successfully loaded {file_path} with shape {df.shape}")
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        exit()

    # Filter the data to include only transactions where the 'year' column is 2023
    df_2023 = df[df['year'] == 2023].copy()
    print(f"Filtered for year 2023. Shape: {df_2023.shape}")

    # Calculate fraud rate per merchant
    # Policy from manual.md Section 7: "Fraud is defined as the ratio of fraudulent volume over total volume."
    
    merchant_stats = []
    merchants = df_2023['merchant'].unique()
    
    for merchant in merchants:
        merchant_txs = df_2023[df_2023['merchant'] == merchant]
        
        # Calculate Total Volume
        total_volume = merchant_txs['eur_amount'].sum()
        
        # Calculate Fraud Volume (where has_fraudulent_dispute is True)
        fraud_txs = merchant_txs[merchant_txs['has_fraudulent_dispute'] == True]
        fraud_volume = fraud_txs['eur_amount'].sum()
        
        # Calculate Fraud Rate (Volume Ratio)
        if total_volume > 0:
            fraud_rate = fraud_volume / total_volume
        else:
            fraud_rate = 0.0
            
        merchant_stats.append({
            'merchant': merchant,
            'total_volume': total_volume,
            'fraud_volume': fraud_volume,
            'fraud_rate': fraud_rate
        })
    
    # Convert to DataFrame for easier handling
    stats_df = pd.DataFrame(merchant_stats)
    
    # Sort by fraud_rate ascending to find the lowest
    stats_df = stats_df.sort_values('fraud_rate', ascending=True)
    
    print("\nFraud Rates per Merchant (2023):")
    print(stats_df[['merchant', 'fraud_rate']])
    
    # Identify the lowest rate
    if not stats_df.empty:
        lowest_rate = stats_df.iloc[0]['fraud_rate']
        lowest_merchant = stats_df.iloc[0]['merchant']
        
        print(f"\nLowest avg fraud rate: {lowest_rate}")
        print(f"Merchant with lowest rate: {lowest_merchant}")
        
        # Final answer output (just the value as requested by "What is the lowest...")
        print(lowest_rate)
    else:
        print("No data found for 2023.")
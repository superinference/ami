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

# Load the data
file_path = '/output/chunk4/data/context/payments.csv'
try:
    df = pd.read_csv(file_path)
    
    # Filter criteria
    # Merchant: Rafa_AI
    # Card Scheme: GlobalCard
    # Time: March - December 2023 (Day of year >= 60 for non-leap year 2023)
    
    # Apply filters
    mask = (
        (df['merchant'] == 'Rafa_AI') & 
        (df['card_scheme'] == 'GlobalCard') & 
        (df['day_of_year'] >= 60)
    )
    
    filtered_df = df[mask]
    
    if filtered_df.empty:
        print("No transactions found matching the criteria.")
    else:
        # Group by aci and calculate mean eur_amount
        result = filtered_df.groupby('aci')['eur_amount'].mean()
        
        # Print the results clearly
        print("Average transaction value grouped by aci for Rafa_AI GlobalCard transactions (March-Dec 2023):")
        for aci_val, avg_amount in result.items():
            print(f"{aci_val}: {avg_amount:.2f}")

except FileNotFoundError:
    print(f"Error: File not found at {file_path}")
except Exception as e:
    print(f"An error occurred: {e}")
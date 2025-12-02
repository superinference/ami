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

# Load the CSV file
file_path = '/output/chunk1/data/context/payments.csv'
df = pd.read_csv(file_path)

# Filter for the year 2023
# Ensure 'year' is treated as numeric for filtering
df['year'] = pd.to_numeric(df['year'], errors='coerce')
df_2023 = df[df['year'] == 2023]

if df_2023.empty:
    print("No data found for year 2023.")
else:
    # Group by 'card_scheme' and calculate the mean of 'has_fraudulent_dispute'
    # 'has_fraudulent_dispute' is boolean (True/False), so mean() calculates the ratio (fraud rate)
    fraud_rates = df_2023.groupby('card_scheme')['has_fraudulent_dispute'].mean()
    
    # Identify the highest fraud rate value
    highest_rate = fraud_rates.max()
    
    # Print the result
    print(highest_rate)
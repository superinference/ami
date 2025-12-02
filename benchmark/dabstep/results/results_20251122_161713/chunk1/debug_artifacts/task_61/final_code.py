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
def analyze_fraud_fluctuation():
    # 1. Load the payments.csv file
    file_path = '/output/chunk1/data/context/payments.csv'
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print("Error: payments.csv not found.")
        return

    # 2. Filter for year 2023
    df_2023 = df[df['year'] == 2023].copy()
    
    if df_2023.empty:
        print("No data found for year 2023.")
        return

    # 3. Create 'month' column from 'day_of_year'
    # Using pandas to_datetime for accurate day_of_year to month conversion
    # Format %Y%j parses Year + DayOfYear (001-366)
    df_2023['date'] = pd.to_datetime(df_2023['year'].astype(str) + df_2023['day_of_year'].astype(str), format='%Y%j')
    df_2023['month'] = df_2023['date'].dt.month

    # 4. Calculate Fraud Volume
    # According to manual.md Section 7: "Fraud is defined as the ratio of fraudulent volume over total volume."
    # Create a column for fraudulent amount (eur_amount if fraud, else 0)
    df_2023['fraud_amount'] = np.where(df_2023['has_fraudulent_dispute'] == True, df_2023['eur_amount'], 0.0)

    # 5. Group by Merchant and Month to calculate Monthly Fraud Rate
    # We sum the fraud amount and total amount per merchant per month
    monthly_stats = df_2023.groupby(['merchant', 'month'])[['fraud_amount', 'eur_amount']].sum().reset_index()

    # Calculate the rate: Fraud Volume / Total Volume
    # Handle division by zero just in case (though unlikely for active merchants)
    monthly_stats['monthly_fraud_rate'] = monthly_stats.apply(
        lambda row: row['fraud_amount'] / row['eur_amount'] if row['eur_amount'] > 0 else 0.0, 
        axis=1
    )

    # 6. Calculate Standard Deviation of Monthly Fraud Rates per Merchant
    merchant_fluctuation = monthly_stats.groupby('merchant')['monthly_fraud_rate'].std()

    # 7. Identify the merchant with the highest fluctuation (std)
    highest_fluctuation_merchant = merchant_fluctuation.idxmax()
    highest_std_value = merchant_fluctuation.max()

    # Debugging output to verify calculations
    # print("\n--- Monthly Fraud Rates Std Dev per Merchant ---")
    # print(merchant_fluctuation)
    # print(f"\nMerchant with highest fluctuation: {highest_fluctuation_merchant} (Std: {highest_std_value:.4f})")

    # 8. Final Output
    # The question asks "Which merchant...", so we print the name.
    print(highest_fluctuation_merchant)

if __name__ == "__main__":
    analyze_fraud_fluctuation()
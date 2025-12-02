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
def analyze_outlier_fraud():
    # Define file path
    payments_path = '/output/chunk1/data/context/payments.csv'

    # Load the payments data
    try:
        df = pd.read_csv(payments_path)
    except FileNotFoundError:
        print(f"Error: File not found at {payments_path}")
        return

    # Filter for transactions where the year is 2023
    # Using .copy() to avoid SettingWithCopyWarning
    df_2023 = df[df['year'] == 2023].copy()

    # Verify data loading
    print(f"Total rows loaded: {len(df)}")
    print(f"Rows with year 2023: {len(df_2023)}")

    if len(df_2023) == 0:
        print("No data found for year 2023.")
        return

    # Ensure eur_amount is float
    df_2023['eur_amount'] = df_2023['eur_amount'].apply(coerce_to_float)

    # Calculate Mean and Standard Deviation for eur_amount
    mean_amount = df_2023['eur_amount'].mean()
    std_amount = df_2023['eur_amount'].std()

    print(f"Mean EUR Amount: {mean_amount:.4f}")
    print(f"Std Dev EUR Amount: {std_amount:.4f}")

    if std_amount == 0:
        print("Standard deviation is 0, cannot calculate Z-scores.")
        return

    # Calculate Z-score
    # Z = (X - mean) / std
    df_2023['z_score'] = (df_2023['eur_amount'] - mean_amount) / std_amount

    # Identify Outliers (Z > 3) and Inliers (Z <= 3)
    outliers = df_2023[df_2023['z_score'] > 3]
    inliers = df_2023[df_2023['z_score'] <= 3]

    # Calculate Fraud Rates
    # has_fraudulent_dispute is boolean, so mean() calculates the proportion of True values
    fraud_rate_outliers = outliers['has_fraudulent_dispute'].mean()
    fraud_rate_inliers = inliers['has_fraudulent_dispute'].mean()

    # Handle cases where there might be no outliers or inliers
    if pd.isna(fraud_rate_outliers):
        fraud_rate_outliers = 0.0
    if pd.isna(fraud_rate_inliers):
        fraud_rate_inliers = 0.0

    # Print detailed statistics for verification
    print("\n--- Analysis Results ---")
    print(f"Outliers count (Z > 3): {len(outliers)}")
    print(f"Inliers count (Z <= 3): {len(inliers)}")
    print(f"Fraud Rate Outliers: {fraud_rate_outliers:.4%}")
    print(f"Fraud Rate Inliers: {fraud_rate_inliers:.4%}")

    # Answer the specific question: "Is the fraud rate of outliers higher than inliers?"
    # Output "yes" or "no"
    print("\n--- Final Answer ---")
    if fraud_rate_outliers > fraud_rate_inliers:
        print("yes")
    else:
        print("no")

if __name__ == "__main__":
    analyze_outlier_fraud()
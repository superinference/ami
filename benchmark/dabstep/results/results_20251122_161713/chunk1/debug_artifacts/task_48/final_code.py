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
def analyze_fraud_correlation():
    file_path = '/output/chunk1/data/context/payments.csv'
    
    try:
        # Load data
        df = pd.read_csv(file_path)
        
        # Verify required columns exist
        required_cols = ['hour_of_day', 'has_fraudulent_dispute']
        for col in required_cols:
            if col not in df.columns:
                print(f"Error: Column '{col}' not found in dataset.")
                return

        # Calculate fraud rate per hour
        # Group by hour (0-23) and calculate mean of boolean fraud flag (True=1, False=0)
        # This gives the probability/rate of fraud for that hour
        hourly_stats = df.groupby('hour_of_day')['has_fraudulent_dispute'].mean().reset_index()
        hourly_stats.columns = ['hour', 'fraud_rate']
        
        # Calculate Pearson correlation coefficient between hour (numeric) and fraud rate
        correlation = hourly_stats['hour'].corr(hourly_stats['fraud_rate'])
        
        # Print intermediate results for verification
        print("Hourly Fraud Rates (First 5 hours):")
        print(hourly_stats.head())
        print(f"\nCalculated Correlation Coefficient: {correlation:.4f}")
        
        # Determine answer based on threshold > 0.50
        # Question: "Is there a strong correlation (>0.50)...?" -> Yes/No
        if correlation > 0.50:
            print("yes")
        else:
            print("no")
            
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    analyze_fraud_correlation()
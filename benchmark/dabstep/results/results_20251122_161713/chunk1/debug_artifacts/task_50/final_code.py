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
def analyze_credit_vs_debit_fraud():
    # Load the payments.csv file
    file_path = '/output/chunk1/data/context/payments.csv'
    try:
        df = pd.read_csv(file_path)
        print(f"Data loaded successfully. Shape: {df.shape}")
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return

    # Ensure boolean columns are treated correctly
    # 'is_credit': True = Credit, False = Debit
    # 'has_fraudulent_dispute': True = Fraud, False = No Fraud
    
    # Calculate fraud rates
    # We use mean() on the boolean column 'has_fraudulent_dispute' which treats True as 1 and False as 0
    # This gives us the proportion of transactions that are fraudulent (the probability)
    
    # Filter for Credit transactions
    credit_txs = df[df['is_credit'] == True]
    credit_fraud_rate = credit_txs['has_fraudulent_dispute'].mean()
    
    # Filter for Debit transactions
    debit_txs = df[df['is_credit'] == False]
    debit_fraud_rate = debit_txs['has_fraudulent_dispute'].mean()
    
    # Print intermediate results for verification
    print(f"Credit Transactions Count: {len(credit_txs)}")
    print(f"Credit Fraud Rate: {credit_fraud_rate:.6f} ({credit_fraud_rate*100:.4f}%)")
    
    print(f"Debit Transactions Count: {len(debit_txs)}")
    print(f"Debit Fraud Rate: {debit_fraud_rate:.6f} ({debit_fraud_rate*100:.4f}%)")
    
    # Answer the specific question: 
    # "Are credit payments more likely to result in a fraudulent dispute compared to debit card payments?"
    if credit_fraud_rate > debit_fraud_rate:
        print("yes")
    else:
        print("no")

if __name__ == "__main__":
    analyze_credit_vs_debit_fraud()
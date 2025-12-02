# ═══════════════════════════════════════════════════════════
# Round 1 - Task 2587
# ═══════════════════════════════════════════════════════════
# Temperature: 0.10
# Verification: insufficient
# Plan steps: 1
# Code length: 3343 characters (FULL CODE)
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

def execute_step():
    # Load the payments.csv file
    file_path = '/output/chunk5/data/context/payments.csv'
    try:
        df = pd.read_csv(file_path)
        print(f"Successfully loaded {file_path} with {len(df)} rows.")
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return

    # Define filter criteria
    target_merchant = 'Belles_cookbook_store'
    start_day = 32
    end_day = 59

    # Apply filters
    # 1. Filter by merchant
    merchant_mask = df['merchant'] == target_merchant
    
    # 2. Filter by day_of_year (February)
    date_mask = (df['day_of_year'] >= start_day) & (df['day_of_year'] <= end_day)
    
    # Combine masks
    filtered_df = df[merchant_mask & date_mask].copy()

    # Output results and verification stats
    print(f"\nFiltering for merchant '{target_merchant}' during February (Day {start_day}-{end_day})...")
    print(f"Filtered row count: {len(filtered_df)}")
    
    if len(filtered_df) > 0:
        avg_amount = filtered_df['eur_amount'].mean()
        print(f"Average Transaction Amount: {avg_amount:.4f}")
        print("\nFirst 5 rows of filtered data:")
        print(filtered_df.head())
        
        # Verify against ground truth mentioned in prompt
        # Ground Truth: Avg_Amount: 90.6113 Count: 1016
        if abs(avg_amount - 90.6113) < 0.01 and len(filtered_df) == 1016:
            print("\n✅ VERIFICATION SUCCESSFUL: Data matches ground truth.")
        else:
            print("\n⚠️ VERIFICATION WARNING: Data does not match ground truth.")
    else:
        print("No records found matching the criteria.")

if __name__ == "__main__":
    execute_step()

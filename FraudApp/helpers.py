import pandas as pd

def identify_separator(file_path: str) -> str:
    """
    Identifies the separator used in a CSV file by reading the first line.

    Parameters:
    file_path (str): The path to the CSV file.

    Returns:
    str: The identified separator character.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        first_line = file.readline()
        if ';' in first_line:
            return ';'
        elif ',' in first_line:
            return ','
        else:
            raise ValueError("Unknown separator in the file.")

def validate_transaction_data(df: pd.DataFrame) -> bool:
    """
    Validates the transaction DataFrame to ensure it contains the required columns
    and that the data types are correct.

    Parameters:
    df (pd.DataFrame): The DataFrame containing transaction data.

    Returns:
    bool: True if validation passes, False otherwise.
    """
    required_columns = {
        'cst_dim_id': 'float64',
        'transdate': object,
        'transdatetime': object,
        'amount': 'float64',
        'docno': 'int64',
        'direction': object,
        'target': 'int64'
    }
    for column, dtype in required_columns.items():
        if column not in df.columns:
            print(f"Missing required column: {column}")
            return False
        if not pd.api.types.is_dtype_equal(df[column].dtype, dtype):
            print(f"Incorrect data type for column: {column}. Expected {dtype}, got {df[column].dtype}")
            return False
    return True

def validate_patterns_data(df: pd.DataFrame) -> object:
    """
    Validates the patterns DataFrame to ensure it contains the required columns
    and that the data types are correct.

    Parameters:
    df (pd.DataFrame): The DataFrame containing patterns data.

    Returns:
    bool: True if validation passes, False otherwise.
    """

    # logins_last_7_days;logins_last_30_days;login_frequency_7d;login_frequency_30d;freq_change_7d_vs_mean;logins_7d_over_30d_ratio;avg_login_interval_30d;std_login_interval_30d;var_login_interval_30d;ewm_login_interval_7d;burstiness_login_interval;fano_factor_login_interval;zscore_avg_login_interval_7d
    required_columns = {
        'transdate': object,
        'cst_dim_id': 'float64',
        'monthly_os_changes': 'int64', 
        'monthly_phone_model_changes': 'int64',
        'last_phone_model_categorical': object,
        'last_os_categorical': object,
        'logins_last_7_days': 'int64',
        'logins_last_30_days': 'int64',
        'login_frequency_7d': 'float64',
        'login_frequency_30d': 'float64',
        'freq_change_7d_vs_mean': 'float64',
        'logins_7d_over_30d_ratio': 'float64',
        'avg_login_interval_30d': 'float64',
        'std_login_interval_30d': 'float64',
        'var_login_interval_30d': 'float64',
        'ewm_login_interval_7d': 'float64',
        'burstiness_login_interval': 'float64',
        'fano_factor_login_interval': 'float64',
        'zscore_avg_login_interval_7d': 'float64'
    }
    missing_columns = []
    incorrect_types = []
    for column, dtype in required_columns.items():
        if column not in df.columns:
            missing_columns.append(column)
        elif not pd.api.types.is_dtype_equal(df[column].dtype, dtype):
            incorrect_types.append((column, dtype, df[column].dtype))
    if missing_columns:
        return {
            "status": "error",
            "message": f"Missing required columns: {', '.join(missing_columns)}"
        }
    if incorrect_types:
        return {
            "status": "error",
            "message": "Incorrect data types for columns: " + ", ".join(
                [f"{col} (expected {exp}, got {got})" for col, exp, got in incorrect_types]
            )
        }
    return {
        "status": "success",
        "message": "Validation passed"
    }

def merge_transaction_pattern_data(transactions: pd.DataFrame, patterns: pd.DataFrame) -> pd.DataFrame:
    merged_df = pd.merge(transactions, patterns, on=['cst_dim_id', 'transdate'], how='left')
    return merged_df

def preprocess_merged_data(df: pd.DataFrame) -> pd.DataFrame:
    df['month'] = pd.to_datetime(df['transdatetime']).dt.month
    df['dayofweek'] = pd.to_datetime(df['transdatetime']).dt.dayofweek
    df['hour'] = pd.to_datetime(df['transdatetime']).dt.hour
    df['day'] = pd.to_datetime(df['transdatetime']).dt.day
    df = df.drop(columns=['transdate', 'transdatetime'])
    df['isweekend'] = df['dayofweek'].apply(lambda x: 1 if x >= 5 else 0)
    def part_of_day(hour):
        if hour < 6:
            return 'night'
        elif hour < 12:
            return 'morning'
        elif hour < 18:
            return 'afternoon'
        else:
            return 'evening'
    df['part_of_day'] = df['hour'].apply(part_of_day)

    df['login_freq_7d_vs_30d_ratio'] = df['login_frequency_7d'] / (df['login_frequency_30d'] + 1e-6)
    df['os_change_ratio'] = df['monthly_os_changes'] / (df['monthly_os_changes'].max() + 1e-6)
    df['device_change_ratio'] = df['monthly_phone_model_changes'] / (df['monthly_phone_model_changes'].max() + 1e-6)
    df['high_login_zscore'] = (df['avg_login_interval_30d'] - df['avg_login_interval_30d'].mean()) / (df['std_login_interval_30d'] + 1e-6)
    df['high_login_zscore_flag'] = (df['high_login_zscore'].abs() > 2).astype(int)
    df['os_device_change'] = df['monthly_os_changes'] * df['monthly_phone_model_changes']
    df['logins_per_hour'] = df['logins_last_7_days'] / (df['hour'] + 1e-6)
    df['bursty_and_frequent'] = df['burstiness_login_interval'] * df['logins_last_7_days']
    df['interval_std_over_mean'] = df['std_login_interval_30d'] / (df['avg_login_interval_30d'] + 1e-6)
    df['ewm_vs_avg'] = df['ewm_login_interval_7d'] / (df['avg_login_interval_30d'] + 1e-6)

    df.drop(columns=['cst_dim_id', 'transdate', 'transdatetime', 'docno', 'target'], inplace=True, errors='ignore')

    return df
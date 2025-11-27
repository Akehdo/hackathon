from fastapi import UploadFile
import pandas as pd

def identify_separator(file: UploadFile) -> str:
    """
    Identifies the separator used in a CSV file by reading the first line.

    Parameters:
    file (UploadFile): The uploaded CSV file.

    Returns:
    str: The identified separator (e.g., ',', ';', '\t').
    """
    first_line = file.readline().decode('cp1251')
    separators = [',', ';', '\t']
    separator_counts = {sep: first_line.count(sep) for sep in separators}
    identified_separator = max(separator_counts, key=separator_counts.get)
    file.seek(0) 
    return identified_separator

def validate_transaction_data(df: pd.DataFrame) -> object:
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
                [f"{col} (edfpected {edfp}, got {got})" for col, edfp, got in incorrect_types]
            )
        }
    return {
        "status": "success",
        "message": "Validation passed"
    }

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
                [f"{col} (edfpected {edfp}, got {got})" for col, edfp, got in incorrect_types]
            )
        }
    return {
        "status": "success",
        "message": "Validation passed"
    }

def merge_transaction_pattern_data(transactions: pd.DataFrame, patterns: pd.DataFrame) -> pd.DataFrame:
    merged_df = transactions.merge(patterns, on=['cst_dim_id', 'transdate'], how='left')
    return merged_df

def preprocess_merged_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how='all', axis=0, subset=['monthly_os_changes', 'monthly_phone_model_changes', 'amount'])
    df['hour'] = pd.to_datetime(df['transdatetime']).dt.hour
    df['dayofweek'] = pd.to_datetime(df['transdatetime']).dt.dayofweek
    df['day'] = pd.to_datetime(df['transdatetime']).dt.day
    df['month'] = pd.to_datetime(df['transdatetime']).dt.month
    df = df.drop(columns=['transdate', 'transdatetime'])
    df['is_weekend'] = df['dayofweek'].apply(lambda df: 1 if df >= 5 else 0)

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
    df['login_acceleration'] = df['login_frequency_7d'] - df['login_frequency_30d']
    df['sudden_activity_spike'] = ((df['logins_last_7_days'] / 7) > (df['logins_last_30_days'] / 30) * 2).astype(int)

    # Recent device/OS change flags
    df['recent_os_change_flag'] = (df['monthly_os_changes'] > 0).astype(int)
    df['recent_device_change_flag'] = (df['monthly_phone_model_changes'] > 0).astype(int)
    df['any_recent_change'] = (df['recent_os_change_flag'] | df['recent_device_change_flag']).astype(int)
    df['multiple_changes'] = ((df['monthly_os_changes'] > 1) | (df['monthly_phone_model_changes'] > 1)).astype(int)

    # Composite risk score (combines multiple signals)
    df['risk_score'] = (
        df['high_login_zscore_flag'] * 2 +
        df['any_recent_change'] * 3 +
        df['sudden_activity_spike'] * 2 +
        (df['logins_7d_over_30d_ratio'] > 0.8).astype(int) +
        df['multiple_changes'] * 4
    )

    # Changes + high activity (very suspicious)
    df['change_with_high_activity'] = ((df['any_recent_change'] == 1) & 
                                    (df['logins_last_7_days'] > df['logins_last_7_days'].quantile(0.75))).astype(int)

    # Suspicious hours (night logins)
    df['risky_hour'] = df['hour'].isin([0, 1, 2, 3, 4, 5, 22, 23]).astype(int)
    df['night_with_change'] = ((df['part_of_day'] == 'night') & (df['any_recent_change'] == 1)).astype(int)

    # Extreme velocity of activity change
    df['extreme_velocity'] = (df['login_acceleration'].abs() > df['login_acceleration'].quantile(0.95)).astype(int)

    # Multidimensional variability score
    df['login_variability_score'] = df['burstiness_login_interval'] * df['interval_std_over_mean'] * (1 + df['os_device_change'])

    # Consistency of behavior (low value = suspicious)
    df['consistency_score'] = 1 / (1 + df['burstiness_login_interval'] + df['os_device_change'])

    # Frequency + variability
    df['freq_variability_product'] = df['logins_last_7_days'] * df['interval_std_over_mean']

    # Deviation from typical behavior
    df['deviation_score'] = abs(df['logins_last_7_days'] - df['logins_last_30_days'] / 4.3) / (df['logins_last_30_days'] / 4.3 + 1e-6)

    # Extreme login frequency values
    df['extreme_login_freq'] = ((df['logins_last_7_days'] > df['logins_last_7_days'].quantile(0.95)) | 
                            (df['logins_last_7_days'] < df['logins_last_7_days'].quantile(0.05))).astype(int)
    

    return df


# Edfample usage:

df1 = pd.DataFrame({
    'transdate': ['2023-01-01'],
    'cst_dim_id': [1.0],
    'monthly_os_changes': [0],
    'monthly_phone_model_changes': [0],
    'last_phone_model_categorical': ['model1'],
    'last_os_categorical': ['os1'],
    'logins_last_7_days': [5],
    'logins_last_30_days': [20],
    'login_frequency_7d': [0.5],
    'login_frequency_30d': [0.67],
    'freq_change_7d_vs_mean': [0.1],
    'logins_7d_over_30d_ratio': [0.25],
    'avg_login_interval_30d': [3600.0],
    'std_login_interval_30d': [600.0],
    'var_login_interval_30d': [360000.0],
    'ewm_login_interval_7d': [3000.0],
    'burstiness_login_interval': [1.2],
    'fano_factor_login_interval': [1.5],
    'zscore_avg_login_interval_7d': [2.0]
})

df2 = pd.DataFrame({
    'cst_dim_id': [1],
    'transdate': ['2023-01-01'],
    'transdatetime': ['2023-01-01 12:00:00'],
    'amount': [100.0],
    'docno': [12345],
    'direction': ['asdkasbd1bh2bsad'],
    'target': [1]
})


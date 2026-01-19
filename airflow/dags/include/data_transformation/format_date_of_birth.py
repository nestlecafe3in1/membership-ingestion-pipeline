import pandas as pd
from airflow.decorators import task

@task
def format_date_of_birth(df: pd.DataFrame) -> pd.DataFrame:
    """
    Formats the 'date_of_birth' column to a 'YYYYMMDD' string format.
    """
    df['date_of_birth'] = df['date_of_birth'].dt.strftime('%Y%m%d')
    return df
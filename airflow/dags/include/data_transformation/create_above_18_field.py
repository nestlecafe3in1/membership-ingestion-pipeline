from airflow.decorators import task
import pandas as pd
@task
def create_above_18_field(df: pd.DataFrame, **context):
    """
    Creates a new field 'above_18' and sets its value to True,
    as the age validation has already been performed in a prior step.
    """
    # Set the 'above_18' field to True for all rows in the dataframe
    df['above_18'] = True
        
    return df
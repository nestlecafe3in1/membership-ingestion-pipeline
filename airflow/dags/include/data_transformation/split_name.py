import pandas as pd
from airflow.decorators import task

@task
def split_name(task_id_for_validation_function: str,**context) -> pd.DataFrame:
    """
    Reads a pandas DataFrame, splits the 'name' column into 'first_name' and 'last_name',
    and handles common name variations like titles, suffixes, and middle names.
    """
    ti = context["ti"]
    source_key = ti.xcom_pull(key= "key", task_ids= task_id_for_validation_function)
    df = pd.read_parquet(source_key, engine='pyarrow')


    # Define lists of common titles and suffixes.
    # You can customize these lists to fit your data.
    titles = ['Mr', 'Mrs', 'Ms', 'Miss', 'Dr', 'Prof']
    suffixes = ['Jr', 'Sr', 'MD', 'DDS', 'PhD', 'II', 'III', 'IV']

     # Strip leading/trailing whitespace from the 'name' column
    df['name_cleaned'] = df['name'].str.strip()

    # Remove titles
    for title in titles:
        df['name_cleaned'] = df['name_cleaned'].str.replace(f'^{title}\\.?\\s+', '', regex=True)

    # 3. Remove suffixes
    for suffix in suffixes:
        df['name_cleaned'] = df['name_cleaned'].str.replace(f'\\s+\\.?{suffix}\\.?$', '', regex=True)

    # 4. Split the cleaned name into parts
    name_parts = df['name_cleaned'].str.split(' ', expand=True)

    # 5. Assign first and last names
    # First name is the first part.
    df['first_name'] = name_parts[0]

    # Last name is the last non-null part.
    df['last_name'] = name_parts.apply(lambda x: x[x.last_valid_index()], axis=1)

    # Drop the intermediate cleaned name column and original name
    df = df.drop(columns=['name_cleaned', 'name']) 


    return df
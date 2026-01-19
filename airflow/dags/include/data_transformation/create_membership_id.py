from airflow.decorators import task
import hashlib
import pandas as pd
import shutil
from pathlib import Path
from include.config import (
    TRANSFORMED_FOLDER, DATA_ELEMENT_VALIDATED_FOLDER
)

from include.utils import (
    get_run_path
)


def generate_hash(row):
    """Helper function to generate hash 256 and returns last_name_{hash} with hash truncated to its first 5 digits"""
    birthday_hash = hashlib.sha256(
        row['date_of_birth'].encode()
        ).hexdigest()
    return f"{row['last_name']}_{birthday_hash[:5]}"

@task
def create_membership_id(df: pd.DataFrame, **context):
    """
    Creates a new field 'membership_id' by combining the last name and a truncated SHA256 hash
    of the birthday. Saves the transformed DataFrame to a 'success_application' folder.
    """
    pipeline_run_id = context["run_id"]
    run_path = get_run_path(pipeline_run_id)
    
    # Initialise directories
    transformed_dir = run_path / TRANSFORMED_FOLDER
    transformed_dir.mkdir(parents=True, exist_ok=True)

    # apply hash 256 onto column membership_id
    df['membership_id'] = df.apply(generate_hash, axis=1)
    

    # Clean up the validated data folder
    val_directory = run_path / DATA_ELEMENT_VALIDATED_FOLDER
    shutil.rmtree(val_directory)

    transformed_key = transformed_dir / "successful_application.parquet"
    df.to_parquet(transformed_key)

    transformed_key = transformed_dir / "successful_application.csv"
    df.to_csv(transformed_key) # For assessor validation
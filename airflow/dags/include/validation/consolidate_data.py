import pandas as pd
from pathlib import Path
from airflow.decorators import task
import shutil

from include.config import (
    CONSOLIDATED_FOLDER, LANDING_FOLDER
)
from include.utils import (
    get_run_path
)

@task
def consolidate_data(**context):
    """
    Consolidates files from the LANDING_FOLDER into a single Parquet file.
    """
    pipeline_run_id = context["run_id"]

    # Initialization and directory Setup
    run_path = get_run_path(pipeline_run_id)
    landing_folder_path = run_path / LANDING_FOLDER

    # init destination directory paths
    consolidated_folder_path = run_path / CONSOLIDATED_FOLDER
    consolidated_folder_path.mkdir(parents=True, exist_ok=True)

    FILE_READERS = {
        '.csv': pd.read_csv,
        '.json': pd.read_json,
    # To add a new file type (e.g., CSV), simply add an entry:
    # '.xml': pd.read_xml_to_df,
    }
    
    # Collect dataframes in landing folder into a list
    dataframes = []
    for file in landing_folder_path.iterdir():
        if not file.is_file():
            continue

        reader_function = FILE_READERS.get(file.suffix.lower())
        if reader_function:
            df = reader_function(file)
            dataframes.append(df)

    # Concatenate all dataframes
    consolidated_df = pd.concat(dataframes, ignore_index=True)

    # write df as parquet to consolidated folder
    consolidated_file_key = consolidated_folder_path / "consolidated_data.parquet"
    consolidated_df.to_parquet(consolidated_file_key)
    # remove landing folder
    shutil.rmtree(landing_folder_path)

    return{
        "file_key": str(consolidated_file_key)
    }
from airflow.decorators import task
from pathlib import Path
import shutil
import os
import logging

from include.config import (
    BASE_DATA_PATH, UPLOADS_FOLDER,
    RUNS_FOLDER, RAW_FOLDER
    )

from include.utils import(
    get_run_path
)

@task
def create_run(**context) -> dict[str, str]:
    """
    Initializes a new pipeline run by creating a dedicated directory and copying 
    source files into it for processing
    """
    pipeline_run_id = context["run_id"]

    # Define the source directory where new, unprocessed files are located.
    source_folder = Path(os.path.join(BASE_DATA_PATH, UPLOADS_FOLDER))

    # Define the specific subfolder for the raw, unprocessed data within the run's directory.
    run_path = get_run_path(pipeline_run_id) 
    destination_folder = Path(run_path) / RAW_FOLDER

    # Ensure directory exists
    destination_folder.mkdir(parents=True, exist_ok=True)

    for item in source_folder.iterdir():
        if item.is_file():
            destination_key = destination_folder / item.name
            
            shutil.move(item, destination_key) 
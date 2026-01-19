from pathlib import Path
from airflow.decorators import task
import json
import shutil
from include.config import (
    BASE_DATA_PATH, RUNS_FOLDER, 
    INVALID_FOLDER, RAW_FOLDER
)
from include.utils import write_json_file

@task
def handle_invalid_files(error_task_id: str, **context):
    """
    Moves invalid files to a designated 'invalid' folder and logs the error.
    """
    ti = context["ti"]
    # Pull the error details dictionary from the upstream task
    error_details = ti.xcom_pull(key='error_details', task_ids = error_task_id)
    pipeline_run_id = context["run_id"]

    # Normalize the input to always be a list for consistent processing
    if not isinstance(error_details, list):
        error_list = [error_details]  # Convert single dict to a list with one item
    else:
        error_list = error_details  # Assume it's already a list

    # Create invalid folder directory 
    invalid_folder_path = Path(BASE_DATA_PATH) / INVALID_FOLDER / pipeline_run_id
    invalid_folder_path.mkdir(parents=True, exist_ok=True)

    for error_details in error_list:
        # Extract the relevant information from the dictionary
        source_key = error_details.get("file_key")
        error_message = error_details.get("error_message")
        source_task_id = error_details.get("source_task_id")
        pipeline_run_id = context["run_id"]

        file_name = Path(source_key).name 
        destination_key = invalid_folder_path / file_name

        # Move the invalid file
        shutil.move(source_key, destination_key)

        # Create and save the error log as a JSON file
        log_content = {
            "pipeline_run_id": pipeline_run_id,
            "pipeline_name": context["dag_run"].dag_id,
            "file_name": file_name,
            "task_id": source_task_id,
            "error_msg": error_message
        }

        log_file_path = invalid_folder_path / f"{file_name}.log"

        # write log content into invalid folder path
        write_json_file(log_file_path, log_content)

import json
from pathlib import Path
from airflow.decorators import task
import shutil
from include.config import (
    RAW_FOLDER, CONFIG_FOLDER, 
    RUNS_FOLDER, CONFIG_FILE_NAME,
    BASE_DATA_PATH, LANDING_FOLDER
)

from include.utils import (
    read_json_file, get_run_path,
    read_configuration_file
)

@task.branch
def verify_file_structure(task_id_for_invalid_files: str, task_id_for_valid_files: str, **context):
    """
    Verifies the file type of all files in the RAW_FOLDER against the configuration file.
    """
    pipeline_run_id = context["run_id"]

    # Init paths
    run_folder_path = get_run_path(pipeline_run_id)
    raw_folder_path = Path(run_folder_path) / RAW_FOLDER
    landing_folder_path = Path(run_folder_path) / LANDING_FOLDER
    
    # Create the landing folder if it doesn't exist
    landing_folder_path.mkdir(parents=True, exist_ok=True)

    config = read_configuration_file(CONFIG_FILE_NAME)

    # retrieve file type conconfiguration from config file
    expected_file_type = config.get("file_type") 

    invalid_files_info = []
    valid_files = []

    # for loop over content in raw folder
    for raw_file_key in raw_folder_path.iterdir():
        # skip if not file or if metadata files are used
        if not raw_file_key.is_file() or raw_file_key.name.startswith('.'):
            continue
        # if file's suffix does not match expected_file type, error is raised into log
        if not raw_file_key.name.endswith(expected_file_type):
            error_info = {
                "file_key": str(raw_file_key),
                "error_message": f"Invalid file type. Expected '{expected_file_type}'",
                "source_task_id": context["task_instance"].task_id
            }
            invalid_files_info.append(error_info)

        else:
            # Move valid files to the run-specific folder
            destination_key = landing_folder_path / raw_file_key.name
            shutil.move(raw_file_key, destination_key)
            valid_files.append(destination_key)

    next_tasks = []

    if invalid_files_info:
        # If there are invalid files, push the details to XComs and add the error handling task
        context["ti"].xcom_push(key="error_details", value=invalid_files_info)
        next_tasks.append(task_id_for_invalid_files)

    if valid_files:
        next_tasks.append(task_id_for_valid_files)

    return next_tasks
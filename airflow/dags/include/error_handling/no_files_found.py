from pathlib import Path
from airflow.decorators import task
import json
from include.config import BASE_DATA_PATH, INVALID_FOLDER
from include.utils import write_json_file

@task
def no_files_found(error_task_id: str, **context):
    """
    Handles the error case where no files are found in the respective folder.
    """
    ti = context["ti"]
    # Pull the error details dictionary from the upstream task (check_for_csv_files)
    error_details = ti.xcom_pull(key='error_details', task_ids = error_task_id)

    # Extract the relevant information from the dictionary
    error_message = error_details.get("error_message")
    source_task_id = error_details.get("source_task_id")
    pipeline_run_id = context["run_id"]

    # Define path for the invalid folder
    invalid_folder_path = Path(BASE_DATA_PATH) / INVALID_FOLDER / pipeline_run_id

    # Create invalid folder directory
    invalid_folder_path.mkdir(parents=True, exist_ok=True)

    # Create and save the error log as a JSON file
    log_content = {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_name": context["dag_run"].dag_id,
        "file_name": "N/A",  # Explicitly N/A for this case
        "task_id": source_task_id,
        "error_msg": error_message
    }

    log_file_path = invalid_folder_path / f"no_files_found.log"

    write_json_file(log_file_path, log_content)

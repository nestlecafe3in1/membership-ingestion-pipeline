from pathlib import Path
from airflow.decorators import task

from include.config import BASE_DATA_PATH, UPLOADS_FOLDER

@task.branch
def check_for_files(task_id_for_create_run:str, task_id_for_no_files_found:str, **context):
    """
    Checks if there are any  files in the uploads folder and pushes an
    XCom with an error message if no files are found.
    """
    source_folder = Path(BASE_DATA_PATH) / UPLOADS_FOLDER

    # check if there is at least one file in the directory.
    has_files = any(file.is_file() for file in source_folder.iterdir())

    if has_files:
        return task_id_for_create_run  # Task ID of the task to run if files are present
    else:
        # If no files are found, push error_details to XCOM
        ti = context["ti"]
        error_details = {
            "error_message": "No CSV files found in the uploads folder.",
            "source_task_id": ti.task_id
        }
        ti.xcom_push(key="error_details", value=error_details)
        return task_id_for_no_files_found
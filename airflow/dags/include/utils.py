import json
from pathlib import Path
from include.config import (
    BASE_DATA_PATH, RUNS_FOLDER, CONFIG_FOLDER
)

def read_json_file(file_path: Path) -> dict:
    """Reads a JSON file and returns its content as a dictionary."""
    with open(file_path, 'r') as file:
        return json.load(file)

def write_json_file(file_path: Path, content: dict):
    """Writes a dictionary to a JSON file."""
    with open(file_path, 'w') as file:
        json.dump(content, file, indent=4)

def get_run_path(pipeline_run_id: str) -> Path:
    """Returns the base path for a given pipeline run."""
    return Path(BASE_DATA_PATH) / RUNS_FOLDER / pipeline_run_id

def read_configuration_file(config_file_name):
    """Returns the content of the specified configuration file."""
    config_file_path = Path(BASE_DATA_PATH) / CONFIG_FOLDER / config_file_name
    config = read_json_file(config_file_path)
    return config
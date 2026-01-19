from io import StringIO
from typing import Any, Dict, Tuple
import shutil
import pandas as pd
from pathlib import Path
from airflow.decorators import task

from include.config import (
    CONSOLIDATED_FOLDER, DATA_ELEMENT_VALIDATED_FOLDER,
    REJECTED_FOLDER, CONFIG_FILE_NAME
)
from include.utils import get_run_path, read_configuration_file, write_json_file

from include.validation.validation_functions import(
    validate_age_over, validate_email_domain,
    validate_length, validate_mandatory, validate_regex
)

# --------- Dtype mapping from config keywords to pandas dtypes ----------
_PANDAS_DTYPES = {
    "string": "string",
    "int": "Int64",            # nullable integer
    "int64": "Int64",
    "float": "float64",
    "float64": "float64",
    "date": "datetime64[ns]",
}
# --------- Explicit registry of available validation functions. ---------
VALIDATION_FUNCTIONS = {
    "validate_mandatory": validate_mandatory,
    "validate_email_domain": validate_email_domain,
    "validate_length": validate_length,
    "validate_age_over": validate_age_over,
    "validate_regex": validate_regex
}

def _coerce_dtypes(df: pd.DataFrame, validation_rules: Dict[str, Any]) -> pd.DataFrame:
    """Convert columns to pandas dtypes based on config (string/int/float/date)."""
    columnar_rules = validation_rules.get("data_validation", {})

    # build a {column: datatype} map
    dtype_map = {} # Map for storing non-date dtype conversions
    date_columns = [] # Track columns that should be parsed as dates separately

    # retrive datatype of column from individual columnar rules
    for column, rules in columnar_rules.items():
        data_type = str(rules.get("data_type", "")).lower()
        if not data_type:
            continue

        if data_type == "date":
            # Handle dates separately since astype() won't parse strings to datetime
            date_columns.append(column)
        else:
            # Look up pandas dtype for non-date types (int, float, string, etc.)
            dtype_map[column] = _PANDAS_DTYPES.get(data_type, None)

    # Apply non-date dtype coercions
    if dtype_map:
        df = df.astype({column: dtype for column, dtype in dtype_map.items() if dtype})

    # Parse date columns with safe coercion
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format = "mixed", errors = 'coerce') # error in converting to date will lead to null

    return df

def _apply_rules(df: pd.DataFrame, rules_json: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Apply validation rules column-by-column and split the DataFrame into valid/invalid rows.
    Contract:
      - `rules_json` is expected to contain a "data_validation" mapping:
          {
            "<column_name>": {
               "data_type": "string|int|float|date",   # handled elsewhere; ignored here
               "<rule_fn_name>": <rule_params>,        # for each custom rule
               ...
            },
            ...
          }

      - Each rule function must be available (e.g., in globals() or a registry) under <rule_fn_name>
        and have the signature: rule_fn(value, params) -> bool
          * Returns True if the value PASSES the rule.
          * Returns False if the value FAILS the rule.
    """
    error_log = StringIO()

    # Track indices of rows that fail any rule; set ensures uniqueness
    invalid_row_indices = set()

    # retrieve data_validation rules
    rules = rules_json.get("data_validation", {})

    # Iterate over each column and its corresponding rules from the configuration.
    # 'rules' is a dictionary like: {'column_name': {'rule_name_1': params}, {'rule_name_2': params}}
    for column, col_rules in rules.items():

        # For the current column, iterate over each specific validation rule.
        # e.g., 'rule_name' could be 'min_length' and 'params' could be 5.
        for rule_name, params in col_rules.items():

            # Skip "data_type" if present as it's coerced separately
            if rule_name == "data_type":
                continue
            
            # Retrieve validation function from registry
            func = VALIDATION_FUNCTIONS.get(rule_name)

            # Safety check: If the rule name from the config doesn't exist in our
            # function registry, log an error and move to the next rule.
            if not callable(func):
                error_log.write(f"[CONFIG] Undefined function '{rule_name}' for column '{column}'.\n")
                continue
            
            # Apply the validation function row-by-row, if row is None/Nan, row is invalid,
            # else it will call the valdation function and return False if value passes the rule
            mask_invalid = df[column].apply(lambda x: not func(x, params))

            failed = df[mask_invalid]

            # if there are failed rows
            if not failed.empty:
                invalid_row_indices.update(failed.index.tolist())

    # Split valid/invalid
    invalid_idx_sorted = sorted(list(invalid_row_indices))

    # Build invalid subset. If none, return an empty frame with same schema.
    df_invalid = df.loc[invalid_idx_sorted] if invalid_idx_sorted else df.iloc[0:0].copy()

    df_valid = df.drop(index=invalid_idx_sorted)

    # Materialize error report as a DataFrame (optional but handy)
    error_text = error_log.getvalue()

    return df_valid, df_invalid, error_text

@task.branch
def validate_application(struct_info, task_id_for_valid_application:str,
        task_id_for_invalid_application:str,  **context):
    """
    Read consolidated Parquet, enforce data types per config, validate columns by rule
    """
    pipeline_run_id = context["run_id"]
    source_key = struct_info.get("file_key")

    run_path = get_run_path(pipeline_run_id)
    
    # INIT validated and rejected directories
    validated_dir     = run_path / DATA_ELEMENT_VALIDATED_FOLDER
    rejected_dir      = run_path / REJECTED_FOLDER
    validated_dir.mkdir(parents=True, exist_ok=True)
    rejected_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(source_key)
    rules = read_configuration_file(CONFIG_FILE_NAME)

    # Enforce data types based on config
    df = _coerce_dtypes(df, rules)

    # Generate & apply validation rules (functions are imported above), separate rows
    df_valid, df_invalid, error_text = _apply_rules(df, rules)

    # if there is invalid application, send them to separate folder
    if not df_invalid.empty:
        # write invalid_df as parquet to rejected folder in runs folder
        df_invalid.to_parquet(rejected_dir / "rejected_applications.parquet", index=False) 

        # write error logs as json file together with rejected dataframe
        log_file_path = rejected_dir / f"rejected_applications.log"
        write_json_file(log_file_path, error_text) 

    # Clean up the consolidated data folder regardless of the outcome
    shutil.rmtree(Path(source_key).parent)

    # if there are valid rows
    if not df_valid.empty:
        # If there are valid rows, save them and proceed to the next task.
        destination_key = validated_dir / "approved_applications.parquet"
        df_valid.to_parquet(destination_key, index=False)
        print("df_valid_info", df_valid.shape)

        # push destination key to next task
        ti = context["ti"]
        ti.xcom_push(key="key", value = str(destination_key))
        return task_id_for_valid_application
    else:
        # If there are no valid rows, branch to the stop task.
        print("No valid data found. Ending this pipeline branch.")
        return task_id_for_invalid_application
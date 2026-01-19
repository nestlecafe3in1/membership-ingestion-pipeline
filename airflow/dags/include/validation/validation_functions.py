from datetime import datetime
from typing import Any, Dict, Optional
import pandas as pd
import re

# ---------- Generic helpers (rule names must match JSON) ----------

def validate_mandatory(value: Any, param: Optional[dict] = None) -> bool:
    """True if value is present/non-empty."""
    if pd.isna(value):
        return False
    return str(value).strip() != ""

def validate_email_domain(value: Any, params: Dict[str, Any]) -> bool:
    """True if email ends with one of the allowed domain suffixes."""
    if pd.isna(value):
        return True # Allow null values
    
    # Get the list of allowed domain suffixes from params, defaulting to an empty list.
    allowed = params.get("allowed", [])

    # Convert the input to a lowercase string with whitespace removed from both ends.
    string_value = str(value).strip().lower()

    # Check if the email ends with any of the allowed domain suffixes (case-insensitive).
    return any(string_value.endswith(domain.lower()) for domain in allowed)

def validate_length(value: Any, params: Dict[str, Any]) -> bool:
    """True if len(value) in [min,max]."""
    if pd.isna(value):
        return True # Allow null values
    
    allow_whitespace = params.get("allow_whitespace")
    if not allow_whitespace:
        # if whitespace is not allowed, program will remove empty spaces to calculate length
        string_value = str(value).replace(' ', '') 

    string_value = str(value)
    string_length = len(string_value)

    # check if record value is within min and max range
    min, max = params.get("min"), params.get("max")
    if min is not None and string_length < min:
        return False
    if max is not None and string_length > max:
        return False
    return True

def validate_age_over(value: Any, params: Dict[str, Any]) -> bool:
    """
    True if age (as of 'as_of_date') >= params['age'].
    """
    if pd.isna(value):
        return False # cant allow null values as need to verify date
    as_of_str = params.get("as_of_date")
    min_age = int(params.get("age", 0))
    try:
        # Parse both the date of birth and the "as of" date
        date_of_birth = value # datetime already converted in _coerce_dtypes function
        as_of_date = pd.to_datetime(as_of_str) 
        if pd.isna(date_of_birth) or pd.isna(as_of_date):
            return False
        
        # Compute age in whole years.
        # Subtract 1 if the birthday hasn't occurred yet this year.
        years = as_of_date.year - date_of_birth.year - ((as_of_date.month, as_of_date.day) < (date_of_birth.month, date_of_birth.day))
        
        # Check if computed age meets or exceeds the minimum age
        return years >= min_age
    except Exception:
        return False

def validate_regex(value: Any, params:str) -> bool:
    """True if regex matches."""
    if pd.isna(value):
        return True # Allow null values
    if not params:
        return False
    # checks if value matches regex params
    return re.fullmatch(params, str(value)) is not None


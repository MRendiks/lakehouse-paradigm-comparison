from datetime import datetime

def format_date(date_str: str) -> str:
    """Format string to ISO 8601"""
    return datetime.fromisoformat(date_str).isoformat()

def clean_string(input_str: str) -> str:
    """Remove special characters from string"""
    return "".join(e for e in input_str if e.isalnum() or e.isspace())

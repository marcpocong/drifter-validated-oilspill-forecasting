"""
Data models and schemas for the application.
"""
from dataclasses import dataclass
from typing import Optional

@dataclass
class ValidationResult:
    """Class for keeping track of validation results."""
    recipe_name: str
    ncs_score: float
    map_file: Optional[str] = None
    error: Optional[str] = None

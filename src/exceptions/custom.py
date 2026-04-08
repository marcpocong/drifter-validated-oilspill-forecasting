"""
Custom exceptions for the application.
"""

class ValidationPipelineError(Exception):
    """Base exception for validation pipeline."""
    pass

class DataLoadingError(ValidationPipelineError):
    """Raised when data loading fails."""
    pass

class SimulationError(ValidationPipelineError):
    """Raised when simulation fails."""
    pass

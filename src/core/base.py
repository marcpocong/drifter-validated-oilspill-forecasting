"""
Core application definitions.
"""
from abc import ABC, abstractmethod

class BaseService(ABC):
    """Abstract base class for services."""
    
    @abstractmethod
    def run(self):
        """Execute the service logic."""
        pass

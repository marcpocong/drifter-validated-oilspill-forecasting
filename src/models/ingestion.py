"""
Data models for the Ingestion Service.
"""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime

@dataclass
class DownloadStatus:
    """Status of a single download operation."""
    path: Optional[str] = None
    status: str = "PENDING"
    error: Optional[str] = None

@dataclass
class IngestionManifest:
    """Manifest of the ingestion process."""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    config: Dict[str, str] = field(default_factory=dict)
    downloads: Dict[str, Any] = field(default_factory=dict)

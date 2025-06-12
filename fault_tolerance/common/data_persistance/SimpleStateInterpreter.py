import json
import logging
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface
from typing import Any, Dict, List

class SimpleStateInterpreter(StateInterpreterInterface):
    """
    State interpreter for consumer data, handling message information.
    Implements the StateInterpreterInterface.
    """
    
    def __init__(self, status_completed="COMPLETED_"):
        """
        Initialize the SimpleStateInterpreter.
        
        Args:
            status_completed (str): The status string that indicates a completed operation
        """
        self.STATUS_COMPLETED = status_completed
    
    def format_data(self, data: Any) -> str:
        """
        Format business data for storage.
        
        This method only handles business data formatting, not WAL structure.
        The WAL's responsibility is to add any needed WAL-specific metadata.
        
        Args:
            data: Business data to format
            
        Returns:
            str: Formatted business data ready for storage
        """
        # Create standardized structure for business data
        wal_structure = {
            "data": data,  # Store original business data unchanged
            "_metadata": {}  # Reserved for WAL to add its metadata
        }
            
        # Serialize the structure
        return json.dumps(wal_structure)

    def parse_data(self, content: str) -> Any:
        """
        Parse stored data, separating business data from WAL implementation details.
        
        Args:
            content: Stored content to parse
            
        Returns:
            Any: Parsed business data as a dictionary
        """
        return json.loads(content).get("data", {})
    
    def merge_data(self, data_entries: Any) -> Any:
        """
        Merge multiple data entries by adding up their values.
        
        Args:
            data_entries: List of business data entries to merge
            
        Returns:
            Dict[str, Any]: Merged business data with sum of all values
        """
        
        return data_entries

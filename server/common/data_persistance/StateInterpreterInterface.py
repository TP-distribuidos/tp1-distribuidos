import abc
from typing import Any

class StateInterpreterInterface(abc.ABC):
    """
    Interface for interpreting and formatting data for persistence.
    """
    
    @abc.abstractmethod
    def format_data(self, data: Any) -> str:
        """
        Format data for storage.
        
        IMPORTANT: This method MUST return a JSON string representing a dictionary 
        with exactly these two keys:
        - "data": Contains the business data to be stored
        - "_metadata": An empty dict (or with custom metadata) that WAL will populate
        
        Example:
        {
            "data": your_business_data,
            "_metadata": {}
        }
        
        Args:
            data: Business data to format
            
        Returns:
            str: JSON string with the required structure
        """
        pass
    
    @abc.abstractmethod
    def parse_data(self, content: str) -> Any:
        """
        Parse stored data.
        
        Args:
            content: Stored content to parse
            
        Returns:
            Any: Parsed data
        """
        pass
    
    @abc.abstractmethod
    def merge_data(self, data_entries: Any) -> Any:
        """
        Merge multiple data entries into a single state.
        Used for checkpointing or combining multiple logs.
        
        Args:
            data_entries: Dictionary mapping entry IDs to their data
            
        Returns:
            Any: Merged data representing combined state
        """
        pass

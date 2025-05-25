import abc
from typing import Any, Tuple, Dict

class StateInterpreterInterface(abc.ABC):
    """
    Interface for interpreting and formatting data for persistence.
    """
    
    @abc.abstractmethod
    def format_data(self, data: Any) -> str:
        """
        Format data for storage.
        
        Args:
            data: Data to format
            
        Returns:
            str: Formatted data from worker to for DataPersistance class
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
    def merge_data(self, data_entries: Dict[str, Any]) -> Any:
        """
        Merge multiple data entries into a single state.
        Used for checkpointing or combining multiple logs.
        
        Args:
            data_entries: Dictionary mapping entry IDs to their data
            
        Returns:
            Any: Merged data representing combined state
        """
        pass

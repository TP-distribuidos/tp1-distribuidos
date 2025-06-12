import abc
from typing import Any, Optional

class DataPersistenceInterface(abc.ABC):
    """
    Interface for data persistence operations.
    All persistence algorithms must implement this interface.
    """
    
    @abc.abstractmethod
    def persist(self, client_id: str, data: Any, operation_id: int) -> bool:
        """
        Persist data for a client.
        
        Args:
            client_id: Client identifier
            data: Data to persist
            operation_id: Unique operation identifier as integer
            
        Returns:
            bool: True if successfully persisted
        """
        pass
    
    @abc.abstractmethod
    def retrieve(self, client_id: str) -> Any:
        """
        Retrieve data for a client.
        
        Args:
            client_id: Client identifier
            
        Returns:
            Any: Retrieved data or None if not found
        """
        pass
    
    @abc.abstractmethod
    def clear(self, client_id: str) -> bool:
        """
        Clear all data for a client.
        
        Args:
            client_id: Client identifier
            
        Returns:
            bool: True if successfully cleared
        """
        pass

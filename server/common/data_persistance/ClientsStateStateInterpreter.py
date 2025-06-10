import json
import logging
from typing import Any, Dict, List
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class ClientsStateStateInterpreter(StateInterpreterInterface):
    """
    State interpreter for tracking client states in the join worker.
    Handles formatting, parsing and merging of client state data.
    """
    
    def format_data(self, data: Any) -> str:
        """
        Format client state data for storage.
        
        Args:
            data: Client state data {client_id: {'movies_done': bool}}
            
        Returns:
            str: JSON string with the required structure
        """
        logging.info(f"FORMAT DATA: {data}")
        formatted = {
            "data": data, 
            "_metadata": {}
        }
        return json.dumps(formatted)
    
    def parse_data(self, content: str) -> Any:
        """
        Parse stored movie data.
        
        Args:
            content: Stored content to parse
            
        Returns:
            Any: Parsed movie data
        """
        try:
            parsed_data = json.loads(content)
            if isinstance(parsed_data, dict) and "data" in parsed_data:
                return parsed_data["data"]
            return parsed_data
        except json.JSONDecodeError:
            return None
                
    
    def merge_data(self, data_entries: List[Any]) -> Any:
        """
        Merge multiple client state entries.
        Returns True if any entry has 'movies_done' as True.
        
        Args:
            data_entries: List of client state entries to merge
            
        Returns:
            bool: True if any entry has 'movies_done' as True
            
        Raises:
            RuntimeError: If no entry has 'movies_done' as True
        """
        raise RuntimeError("WARNING: data entries has more than one element u fukced up with the format_data function")

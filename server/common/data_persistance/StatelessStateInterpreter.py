import json
from typing import Any, List
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class StatelessStateInterpreter(StateInterpreterInterface):
    """
    A stateless implementation of the StateInterpreterInterface.
    This interpreter doesn't actually store or process any data - it's used
    when we only need the WAL's counter and deduplication capabilities.
    """
    
    def format_data(self, data: Any) -> str:
        """
        Format data for storage - returns an empty data structure in the required format.
        
        Args:
            data: Business data to format (ignored in this implementation)
            
        Returns:
            str: JSON string with the required empty structure
        """
        # Return the minimal required structure with empty data
        formatted = {
            "data": {},  
            "_metadata": {}
        }
        return json.dumps(formatted)
    
    def parse_data(self, content: str) -> Any:
        """
        Parse stored data - just returns the parsed JSON.
        
        Args:
            content: Stored content to parse
            
        Returns:
            Any: Parsed data
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
        Merge multiple data entries - just returns an empty object since we don't store state.
        
        Args:
            data_entries: List of data entries to merge
            
        Returns:
            Any: Empty object
        """
        return {}
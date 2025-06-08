import json
import logging
from typing import Any, List
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class StateInterpreter(StateInterpreterInterface):
    """
    State interpreter for the Max-Min Worker.
    Handles formatting, parsing and merging the max and min movie data.
    """
    
    def format_data(self, data: Any) -> str:
        """
        Format data for storage.
        
        Args:
            data: Business data to format 
            
        Returns:
            str: JSON string with the required structure
        """
        formatted = {
            "data": data,  
            "_metadata": {}
        }
        return json.dumps(formatted)
    
    def parse_data(self, content: str) -> Any:
        """
        Parse stored data.
        
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
        Merge multiple data entries into a single state.
        For max/min, we need to find the absolute max and min across all entries.
        
        Args:
            data_entries: List of data entries to merge
            
        Returns:
            Any: Merged data with global max and min
        """
        
        if not data_entries:
            return {"max": None, "min": None}
        
        # Initialize with the first entry
        best_max = data_entries[0].get('max')
        best_min = data_entries[0].get('min')
        
        # Find highest max and lowest min across all entries
        for entry in data_entries[1:]:
            current_max = entry.get('max')
            current_min = entry.get('min')
            
            # Update max if we found a higher rating
            if (current_max and best_max and 
                current_max.get('avg', 0) > best_max.get('avg', 0)):
                best_max = current_max
            
            # Update min if we found a lower rating
            if (current_min and best_min and 
                current_min.get('avg', float('inf')) < best_min.get('avg', float('inf'))):
                best_min = current_min
        
        return {
            "max": best_max,
            "min": best_min
        }
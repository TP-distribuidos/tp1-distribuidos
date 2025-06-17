import json
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class StateInterpreter(StateInterpreterInterface):
    """
    StateInterpreter for the Boundary service.
    Handles mapping between client IPs and UUIDs.
    """
    
    def format_data(self, data):
        """
        Format IP-to-UUID mapping for storage.
        
        Args:
            data: Dictionary with IP as key and UUID as value
            
        Returns:
            str: JSON string with the required structure
        """
        if not isinstance(data, dict):
            raise ValueError("Data must be a dictionary with IP as key and UUID as value")
        
        # Create the required structure with data and empty metadata
        formatted_data = {
            "data": data,
            "_metadata": {}
        }
        
        return json.dumps(formatted_data)
    
    def parse_data(self, content):
        """
        Parse stored IP-to-UUID mapping.
        
        Args:
            content: Stored content to parse
            
        Returns:
            dict: The IP-to-UUID mapping
        """
        parsed = json.loads(content)
        
        # If the parsed data follows the required structure, return the business data
        if isinstance(parsed, dict) and "data" in parsed:
            return parsed["data"]
        
        # Otherwise, assume the entire content is the business data
        return parsed
    
    def merge_data(self, data_entries):
        """
        Merge multiple IP-to-UUID mappings.
        When we have multiple entries (perhaps from different logs or checkpoints),
        we combine them into a single mapping.
        
        Args:
            data_entries: List of dictionaries with IP-to-UUID mappings
            
        Returns:
            dict: Merged IP-to-UUID mapping
        """
        # Initialize an empty result dictionary
        result = {}
        
        # For each entry in the list, update our result
        for entry in data_entries:
            if isinstance(entry, dict):
                # Update the result with the entry's mappings
                result.update(entry)
        
        return result
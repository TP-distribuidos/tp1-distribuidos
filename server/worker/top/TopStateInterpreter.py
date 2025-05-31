import json
import logging
from collections import defaultdict
from typing import Dict, Any, List
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class TopStateInterpreter(StateInterpreterInterface):
    """
    State interpreter for the Top Actors worker.
    Handles parsing, formatting, and merging actor count data.
    """
    
    def format_data(self, data: List[Dict[str, Any]]) -> str:
        """
        Format actor count data for WAL storage.
        
        Args:
            data: List of actor data dictionaries from count workers
                  Format: [{"name": "Actor1", "count": 5}, {"name": "Actor2", "count": 3}]
            
        Returns:
            str: JSON string with 'data' and '_metadata' fields as required by WAL
        """
        # Convert the list of actor data to a dictionary for efficient storage and merging
        actor_counts = {}
        for actor_data in data:
            name = actor_data.get("name")
            count = actor_data.get("count", 0)
            if name:
                # Add to existing count (in case of duplicates in this batch)
                actor_counts[name] = actor_counts.get(name, 0) + count
        
        # Create WAL-compliant structure
        wal_structure = {
            "data": actor_counts,  # Store as dict: {"Actor1": 5, "Actor2": 3}
            "_metadata": {}  # Reserved for WAL to add its metadata
        }
        
        return json.dumps(wal_structure)
    
    def parse_data(self, content: str) -> Dict[str, int]:
        """
        Parse stored actor count data.
        
        Args:
            content: Stored content to parse
            
        Returns:
            Dict[str, int]: Actor counts as {"actor_name": count}
        """
        try:
            data = json.loads(content)
            
            if isinstance(data, dict):
                # Handle WAL format with proper structure
                if "data" in data:
                    business_data = data["data"]
                    if isinstance(business_data, dict):
                        return business_data
                
                # Handle direct actor counts (for backward compatibility)
                # Filter out WAL metadata fields
                filtered_data = {k: v for k, v in data.items() if k != "_metadata"}
                return filtered_data
                
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing actor count data: {e}")
            
        return {}
    
    def merge_data(self, data_entries: List[Any]) -> Dict[str, int]:
        """
        Merge multiple actor count data entries by summing counts for each actor.
        
        Args:
            data_entries: List of actor count dictionaries to merge
            
        Returns:
            Dict[str, int]: Merged actor counts
        """
        merged_counts = defaultdict(int)
        
        for entry in data_entries:
            if isinstance(entry, dict):
                # Each entry should be a dict of actor_name -> count
                for actor_name, count in entry.items():
                    if isinstance(count, (int, float)):
                        merged_counts[actor_name] += int(count)
                    else:
                        logging.warning(f"Skipping non-numeric count for actor {actor_name}: {count}")

        # Convert back to regular dict
        result = dict(merged_counts)
        logging.debug(f"Merged {len(data_entries)} entries into {len(result)} unique actors")
        return result

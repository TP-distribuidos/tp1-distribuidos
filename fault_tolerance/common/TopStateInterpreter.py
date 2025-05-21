import json
from collections import defaultdict
from typing import Dict, Any, List
from fault_tolerance.common.StateInterpreterInterface import StateInterpreterInterface

class TopStateInterpreter(StateInterpreterInterface):
    """
    State interpreter for the Top Actors worker.
    Handles parsing, formatting, and merging actor count data.
    """
    
    def format_data(self, data: Any) -> str:
        """
        Format actor data for storage.
        
        Args:
            data: List of actor data dictionaries
            
        Returns:
            str: Formatted JSON strings, one per line
        """
        formatted_lines = []
        
        if isinstance(data, list):
            for actor_data in data:
                formatted_lines.append(json.dumps(actor_data))
        elif isinstance(data, dict):
            # Handle aggregated data case
            for actor_name, count in data.items():
                formatted_lines.append(json.dumps({"name": actor_name, "count": count}))
        else:
            formatted_lines.append(json.dumps(data))
            
        return "\n".join(formatted_lines)
    
    def parse_data(self, content: str) -> Dict[str, int]:
        """
        Parse stored actor data.
        
        Args:
            content: Stored content to parse
            
        Returns:
            Dict[str, int]: Dictionary mapping actor names to counts
        """
        actor_counts = defaultdict(int)
        
        # Split content into lines and process each one
        lines = content.strip().split("\n")
        for line in lines:
            if not line:
                continue
                
            try:
                data = json.loads(line)
                
                # Handle expected format {"name": actor_name, "count": count}
                if isinstance(data, dict) and "name" in data and "count" in data:
                    actor_name = data["name"]
                    count = data["count"]
                    actor_counts[actor_name] += count
                    
            except json.JSONDecodeError:
                # Skip invalid lines
                continue
                
        return dict(actor_counts)
    
    def merge_data(self, data_entries: Dict[str, Any]) -> Dict[str, int]:
        """
        Merge multiple data entries into a single state.
        
        Args:
            data_entries: Dictionary mapping entry IDs to parsed data
            
        Returns:
            Dict[str, int]: Merged actor counts
        """
        merged_counts = defaultdict(int)
        
        # Process each entry
        for entry_id, actor_counts in data_entries.items():
            if not actor_counts:
                continue
                
            # Each entry should be a dict mapping actor names to counts
            if isinstance(actor_counts, dict):
                for actor_name, count in actor_counts.items():
                    merged_counts[actor_name] += count
                    
        return dict(merged_counts)

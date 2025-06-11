import json
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface
from collections import defaultdict
import heapq

class StateInterpreter(StateInterpreterInterface):
    """State interpreter for the Top Actors worker."""
    
    def format_data(self, data):
        """
        Format data for storage.
        """
        formatted_data = {
            "data": data,
            "_metadata": {}
        }
        return json.dumps(formatted_data)
    
    def parse_data(self, content):
        """
        Parse stored data.
        """
        return json.loads(content)
    
    def merge_data(self, data_entries):
        """
        Merge multiple data entries into a single state.
        Combines all actor counts into a single dictionary.
        """
        actor_counts = defaultdict(int)
        
        for entry in data_entries:
            # Handle different data structures that might come in
            if isinstance(entry, list):
                # Direct list of actor data
                for actor_data in entry:
                    name = actor_data.get("name")
                    count = actor_data.get("count", 0)
                    if name:
                        actor_counts[name] += count
            elif isinstance(entry, dict):
                # Check if this is a data container with nested actor data
                if "data" in entry and isinstance(entry["data"], list):
                    for actor_data in entry["data"]:
                        name = actor_data.get("name")
                        count = actor_data.get("count", 0)
                        if name:
                            actor_counts[name] += count
                else:
                    # Handle direct dict of actor counts or other formats
                    for key, value in entry.items():
                        if key != "_metadata" and isinstance(value, (int, float)):
                            actor_counts[key] += value
        
        return actor_counts
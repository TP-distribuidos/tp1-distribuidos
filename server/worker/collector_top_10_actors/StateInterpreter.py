import json
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class StateInterpreter(StateInterpreterInterface):
    """
    State interpreter for the Top 10 Actors worker.
    Handles formatting and parsing of actor data.
    """
    
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
        Combines all actor data and returns the top 10 actors.
        """
        # Dictionary to store actor counts
        actors_dict = {}
        
        # Process each data entry
        for entry in data_entries:
            # Each entry might be a list of actors or a dictionary
            if isinstance(entry, list):
                # Process list of actors
                for actor_info in entry:
                    name = actor_info.get("name")
                    count = actor_info.get("count", 0)
                    
                    if not name:
                        continue
                    
                    # Update or add the actor count
                    if name in actors_dict:
                        # Take the higher count if we see the same actor twice
                        actors_dict[name] = max(actors_dict[name], count)
                    else:
                        actors_dict[name] = count
            elif isinstance(entry, dict):
                # Each data entry could contain actor data directly
                for actor_data in entry.get("data", []):
                    name = actor_data.get("name")
                    count = actor_data.get("count", 0)
                    
                    if not name:
                        continue
                    
                    # Update or add the actor count
                    if name in actors_dict:
                        actors_dict[name] = max(actors_dict[name], count)
                    else:
                        actors_dict[name] = count
        
        # Sort actors by count (descending) and take top 10
        top_actors = sorted(
            [{"name": name, "count": count} for name, count in actors_dict.items()],
            key=lambda x: (-x["count"], x["name"])  # Sort by count desc, then name asc
        )[:10]
        
        return top_actors
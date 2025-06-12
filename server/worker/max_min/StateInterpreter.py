import json
import logging
from typing import Any, Dict, List
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class StateInterpreter(StateInterpreterInterface):
    """
    State interpreter for the Max-Min Worker.
    Handles formatting, parsing and merging the max and min movie data.
    """
    
    def format_data(self, data: Any) -> str:
        """
        Format data for storage. Aggregates repeated movies if needed.
        
        Args:
            data: Movie data to format and process
            
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
        Handles list format data from average_movies_by_rating worker.
        
        Args:
            data_entries: List of data entries to merge (each entry is a list of movies)
            
        Returns:
            Any: Merged data with all movies aggregated
        """
        if not data_entries:
            return {}
        
        # Create a dictionary to hold all movies, indexed by ID
        all_movies = {}
        
        # Process all entries
        for entry in data_entries:
            # Each entry is a list of movie objects
            if isinstance(entry, list):
                for movie in entry:
                    movie_id = movie.get('id')
                    
                    if not movie_id:
                        logging.warning(f"Skipping movie without ID: {movie}")
                        continue
                    
                    # Initialize movie if not seen before
                    if movie_id not in all_movies:
                        all_movies[movie_id] = {
                            'sum': 0,
                            'count': 0,
                            'id': movie_id,
                            'name': movie.get('name', '')
                        }
                    
                    # Add this movie's sum and count to the total
                    all_movies[movie_id]['sum'] += movie.get('sum', 0)
                    all_movies[movie_id]['count'] += movie.get('count', 0)
            # Handle dictionary format for backward compatibility
            elif isinstance(entry, dict):
                for movie_id, movie_data in entry.items():
                    # Initialize movie if not seen before
                    if movie_id not in all_movies:
                        all_movies[movie_id] = {
                            'sum': 0,
                            'count': 0,
                            'id': movie_id,
                            'name': movie_data.get('name', '')
                        }
                    
                    # Add this entry's sum and count to the total
                    all_movies[movie_id]['sum'] += movie_data.get('sum', 0)
                    all_movies[movie_id]['count'] += movie_data.get('count', 0)
        
        return all_movies
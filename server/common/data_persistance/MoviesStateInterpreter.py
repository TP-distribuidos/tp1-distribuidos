import json
import logging
from typing import Any, Dict, List
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class MoviesStateInterpreter(StateInterpreterInterface):
    """
    State interpreter for movie data in the join worker.
    Handles formatting, parsing and merging of movie data.
    """
    
    def format_data(self, data: Any) -> str:
        """
        Format movie data for storage.
        
        Args:
            data: Movie data either as a dictionary {client_id: {movie_id: movie_name}}
                  or as a list of movie dictionaries [{'id': movie_id, 'name': movie_name}, ...]
            
        Returns:
            str: JSON string with the required structure
        """
        formatted_data = data
        
        formatted_data = {}
        for movie in data:
            movie_id = movie.get('id')
            movie_name = movie.get('name')
            if movie_id and movie_name:
                formatted_data[movie_id] = movie_name
        
        formatted = {
            "data": formatted_data,
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
        Merge multiple movie data entries.
        Combines movies from all entries into a single dictionary mapping movie_id to movie_name.
        
        Args:
            data_entries: List of dictionaries mapping movie_id to movie_name
            
        Returns:
            dict: Merged dictionary of movie_id to movie_name
        """
        if not data_entries:
            return {}
        
        # Create a single merged dictionary of movie_id to movie_name
        merged_movies = {}
        
        # Process each entry, adding all movies to the merged dictionary
        for entry in data_entries:
            for movie_id, movie_name in entry.items():
                merged_movies[movie_id] = movie_name
        
        return merged_movies

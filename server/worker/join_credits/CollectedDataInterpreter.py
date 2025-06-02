import json
import logging
from typing import Dict, Any, List
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class CollectedDataInterpreter(StateInterpreterInterface):
    """
    State interpreter for collected movie data in join_credits worker.
    Handles the movie ID to movie name mappings for each client.
    """
    
    def format_data(self, data: Any) -> str:
        """
        Format movie data for WAL storage.
        
        Args:
            data: List of movie dictionaries from worker
                  Format: [{"id": "movie_id", "name": "Movie Title", ...}, ...]
            
        Returns:
            str: JSON string with data formatted as movie_id -> movie_name mapping
        """
        # Transform the input data into our required format
        movie_data = {}
        
        if isinstance(data, list):
            # Process list of movies into id->name mapping
            for movie in data:
                movie_id = movie.get('id')
                movie_name = movie.get('name')
                if movie_id and movie_name:
                    movie_data[movie_id] = movie_name
            logging.debug(f"Transformed {len(data)} movies into {len(movie_data)} unique movie mappings")
        elif isinstance(data, dict):
            # Data is already in the correct format (or already transformed)
            movie_data = data
        
        # Create WAL-compliant structure
        wal_structure = {
            "data": movie_data,
            "_metadata": {}
        }
        return json.dumps(wal_structure)
    
    def parse_data(self, content: str) -> Dict[str, str]:
        """
        Parse stored movie data.
        
        Args:
            content: Stored content to parse
            
        Returns:
            Dict[str, str]: Dictionary mapping movie IDs to movie names
        """
        try:
            parsed = json.loads(content)
            if isinstance(parsed, dict) and "data" in parsed:
                return parsed["data"]
            return parsed
        except Exception as e:
            logging.error(f"Error parsing movie data: {e}")
            return {}
    
    def merge_data(self, data_entries: List[Dict[str, str]]) -> Dict[str, str]:
        """
        Merge multiple movie data entries, keeping the longest movie name for duplicates.
        
        Args:
            data_entries: List of dictionaries mapping movie IDs to movie names
            
        Returns:
            Dict[str, str]: Combined dictionary with all unique movie ID to name mappings
        """
        if not data_entries:
            return {}
            
        merged_data = {}
        
        # Combine all movie entries, preferring longer names when duplicates exist
        for entry in data_entries:
            if not isinstance(entry, dict):
                continue
                
            for movie_id, movie_name in entry.items():
                merged_data[movie_id] = movie_name

        return merged_data

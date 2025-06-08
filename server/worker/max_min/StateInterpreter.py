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

        # Process and aggregate movies if we have a list of movies
          # Aggregate movies by ID
        processed_movies = {}
        
        for movie in data:
          movie_id = movie.get('id')
          if movie_id is None:
            logging.warning(f"Skipping movie with missing id: {movie}")
            continue
          
          if movie.get('count', 0) <= 0 or movie.get('sum', 0) <= 0:
            logging.warning(f"Skipping movie with zero count or zero sum: {movie}")
            continue
          
          # Initialize movie data if not present
          if movie_id not in processed_movies:
            processed_movies[movie_id] = {
              'sum': 0,
              'count': 0,
              'id': movie_id,
              'name': movie.get('name', '')
            }
          
          # Update the sum and count
          processed_movies[movie_id]['sum'] += movie.get('sum', 0)
          processed_movies[movie_id]['count'] += movie.get('count', 0)

        formatted = {
            "data": processed_movies,
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
      Continues aggregating movie counts and sums across entries.
      
      Args:
          data_entries: List of data entries to merge
          
      Returns:
          Any: Merged data with all movies aggregated
      """
      if not data_entries:
        return {}
      
      # Create a dictionary to hold all movies, indexed by ID
      all_movies = {}
      
      # Process all entries to combine movie data
      for entry in data_entries:
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
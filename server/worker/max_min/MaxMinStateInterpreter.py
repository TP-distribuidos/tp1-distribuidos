import json
import logging
from collections import defaultdict
from typing import Dict, Any, List
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class MaxMinStateInterpreter(StateInterpreterInterface):
    """
    State interpreter for the Max-Min worker.
    Handles parsing, formatting, and merging movie rating data.
    """
    
    def format_data(self, data: List[Dict[str, Any]]) -> str:
        """
        Format movie rating data for WAL storage.
        
        Args:
            data: List of movie data dictionaries
                  Format: [{"id": "movie_id", "name": "Movie Title", "sum": 50, "count": 10, "avg": 5.0}, ...]
            
        Returns:
            str: JSON string with 'data' and '_metadata' fields
        """
        # Convert list of movies into a dictionary for efficient storage and merging
        movie_data = {}
        for movie in data:
            movie_id = movie.get('id')
            if not movie_id:
                continue
                
            movie_data[movie_id] = {
                'name': movie.get('name', ''),
                'sum': movie.get('sum', 0),
                'count': movie.get('count', 0)
            }
            
            # Add avg if provided or calculate it
            if 'avg' in movie:
                movie_data[movie_id]['avg'] = movie['avg']
            elif movie_data[movie_id]['count'] > 0:
                movie_data[movie_id]['avg'] = movie_data[movie_id]['sum'] / movie_data[movie_id]['count']
        
        # Create WAL-compliant structure
        wal_structure = {
            "data": movie_data,
            "_metadata": {}  # Reserved for WAL to add its metadata
        }
        
        return json.dumps(wal_structure)
    
    def parse_data(self, content: str) -> Dict[str, Dict[str, Any]]:
        """
        Parse stored movie rating data.
        
        Args:
            content: Stored content to parse
            
        Returns:
            Dict[str, Dict[str, Any]]: Movie data as {"movie_id": {"name": "Title", "sum": 50, "count": 10, "avg": 5.0}}
        """
        try:
            data = json.loads(content)
            
            if isinstance(data, dict):
                # Handle WAL format with proper structure
                if "data" in data:
                    business_data = data["data"]
                    if isinstance(business_data, dict):
                        return business_data
                
                # Handle direct data (for backward compatibility)
                # Filter out WAL metadata fields
                filtered_data = {k: v for k, v in data.items() if k != "_metadata"}
                return filtered_data
                
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing movie rating data: {e}")
            
        return {}
    
    def merge_data(self, data_entries: List[Dict[str, Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
        """
        Merge multiple movie rating data entries.
        
        Args:
            data_entries: List of movie data dictionaries to merge
            
        Returns:
            Dict[str, Dict[str, Any]]: Merged movie data
        """
        merged_data = {}
        
        for entry in data_entries:
            if not isinstance(entry, dict):
                continue
                
            for movie_id, movie_data in entry.items():
                # Skip if not a dictionary with movie data
                if not isinstance(movie_data, dict):
                    continue
                    
                # Initialize movie in merged data if not present
                if movie_id not in merged_data:
                    merged_data[movie_id] = {
                        'name': movie_data.get('name', ''),
                        'sum': 0,
                        'count': 0
                    }
                
                # Add this entry's sum and count
                merged_data[movie_id]['sum'] += movie_data.get('sum', 0)
                merged_data[movie_id]['count'] += movie_data.get('count', 0)
                
                # Use the most detailed name we have
                if movie_data.get('name', '') and len(movie_data.get('name', '')) > len(merged_data[movie_id]['name']):
                    merged_data[movie_id]['name'] = movie_data.get('name', '')
        
        # Calculate averages for all movies
        for movie_id, movie_data in merged_data.items():
            if movie_data['count'] > 0:
                movie_data['avg'] = movie_data['sum'] / movie_data['count']
        
        return merged_data

import json
import logging
from typing import Any, Dict, List
from common.data_persistance.StateInterpreterInterface import StateInterpreterInterface

class StateInterpreter(StateInterpreterInterface):
    """
    State interpreter for the Average Sentiment Worker.
    Handles formatting, parsing and merging the sentiment data.
    """
    
    def format_data(self, data: Any) -> str:
        """
        Format data for storage.
        
        Args:
            data: Sentiment data to format and process
            
        Returns:
            str: JSON string with the required structure
        """
        # Process the sentiment data from the input
        sentiment_data = {
            "POSITIVE": {"sum": 0, "count": 0},
            "NEGATIVE": {"sum": 0, "count": 0}
        }
        
        if data:
            # Process each movie in the batch
            for movie in data:
                # Look for the sentiment field
                sentiment = movie.get('sentiment')
                
                # Look for the ratio field
                ratio = movie.get('ratio', movie.get('Average', 0))
                
                if sentiment:
                    if sentiment in sentiment_data:
                        # Add to the running total
                        sentiment_data[sentiment]["sum"] += ratio
                        sentiment_data[sentiment]["count"] += 1
        
        formatted = {
            "data": sentiment_data,
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
        Merge multiple sentiment data entries into a single state.
        
        Args:
            data_entries: List of sentiment data entries to merge
            
        Returns:
            Any: Merged sentiment data
        """
        if not data_entries:
            return {
                "POSITIVE": {"sum": 0, "count": 0},
                "NEGATIVE": {"sum": 0, "count": 0}
            }
        
        # Initialize merged sentiment data
        merged_data = {
            "POSITIVE": {"sum": 0, "count": 0},
            "NEGATIVE": {"sum": 0, "count": 0}
        }
        
        # Process all entries
        for entry in data_entries:
            logging.info(f"HERE0 {data_entries}, sadnsad {entry}")

            if isinstance(entry, dict):
                # Handle case where entry is already the sentiment data
                if "POSITIVE" in entry and "NEGATIVE" in entry:
                    logging.info("HERE1")
                    merged_data["POSITIVE"]["sum"] += entry["POSITIVE"].get("sum", 0)
                    merged_data["POSITIVE"]["count"] += entry["POSITIVE"].get("count", 0)
                    merged_data["NEGATIVE"]["sum"] += entry["NEGATIVE"].get("sum", 0)
                    merged_data["NEGATIVE"]["count"] += entry["NEGATIVE"].get("count", 0)
                
                # Handle case where entry contains the data field
                elif "data" in entry and isinstance(entry["data"], dict):
                    logging.info("HERE2")
                    data = entry["data"]
                    if "POSITIVE" in data and "NEGATIVE" in data:
                        merged_data["POSITIVE"]["sum"] += data["POSITIVE"].get("sum", 0)
                        merged_data["POSITIVE"]["count"] += data["POSITIVE"].get("count", 0)
                        merged_data["NEGATIVE"]["sum"] += data["NEGATIVE"].get("sum", 0)
                        merged_data["NEGATIVE"]["count"] += data["NEGATIVE"].get("count", 0)
        
        return merged_data